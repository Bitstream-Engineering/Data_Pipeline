/* Uses dynamic data structures to represent CSV files of various data type */

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// DynamicValue represents a value of any type
type DynamicValue struct {
	StringValue string
	IntValue    int64
	FloatValue  float64
	BoolValue   bool
	TimeValue   time.Time
	Type        ValueType
	IsNull      bool
	/*Nully       bool
	Nyll        bool
	Nilty       bool*/
}

// ValueType represents the type of a dynamic value
type ValueType int

const (
	TypeString ValueType = iota
	TypeInt
	TypeFloat
	TypeBool
	TypeTime
	TypeNull
)

// String returns a string representation of the dynamic value
func (dv DynamicValue) String() string {
	switch dv.Type {
	case TypeString:
		return dv.StringValue
	case TypeInt:
		return strconv.FormatInt(dv.IntValue, 10)
	case TypeFloat:
		return strconv.FormatFloat(dv.FloatValue, 'f', -1, 64)
	case TypeBool:
		return strconv.FormatBool(dv.BoolValue)
	case TypeTime:
		return dv.TimeValue.Format(time.RFC3339)
	case TypeNull:
		return "NULL"
	default:
		return ""
	}
}

// Record represents a row of data with dynamic columns
type Record struct {
	Values map[string]DynamicValue
}

// NewRecord creates a new empty record
func NewRecord() Record {
	return Record{
		Values: make(map[string]DynamicValue),
	}
}

// DataSource interface for reading data
type DataSource interface {
	ReadData(ctx context.Context) (<-chan Record, <-chan error)
}

// DataProcessor interface for processing data
type DataProcessor interface {
	Process(ctx context.Context, in <-chan Record) (<-chan Record, <-chan error)
}

// DataSink interface for writing data
type DataSink interface {
	WriteData(ctx context.Context, in <-chan Record) <-chan error
}

// CSVSource reads data from a CSV file
type CSVSource struct {
	Filename        string
	Delimiter       rune
	HasHeader       bool
	TypeHints       map[string]ValueType // Optional type hints for columns
	TimeFormats     map[string]string    // Optional time formats for time columns
	SkipEmptyValues bool
}

// newDynamicValueFromString attempts to parse a string into the most appropriate type
func newDynamicValueFromString(s string, typeHint ValueType, timeFormat string) DynamicValue {
	// Check for empty or null values
	if s == "" || strings.EqualFold(s, "null") || strings.EqualFold(s, "nil") {
		return DynamicValue{Type: TypeNull, IsNull: true}
	}

	// If type hint is provided, try that first
	if typeHint != TypeNull {
		switch typeHint {
		case TypeInt:
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return DynamicValue{IntValue: i, Type: TypeInt}
			}
		case TypeFloat:
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return DynamicValue{FloatValue: f, Type: TypeFloat}
			}
		case TypeBool:
			lc := strings.ToLower(s)
			if lc == "true" || lc == "yes" || lc == "y" || lc == "1" {
				return DynamicValue{BoolValue: true, Type: TypeBool}
			} else if lc == "false" || lc == "no" || lc == "n" || lc == "0" {
				return DynamicValue{BoolValue: false, Type: TypeBool}
			}
		case TypeTime:
			format := timeFormat
			if format == "" {
				format = time.RFC3339
			}
			if t, err := time.Parse(format, s); err == nil {
				return DynamicValue{TimeValue: t, Type: TypeTime}
			}
		case TypeString:
			return DynamicValue{StringValue: s, Type: TypeString}
		}
	}

	// Auto detection in order of specificity
	// Try boolean
	lc := strings.ToLower(s)
	if lc == "true" || lc == "yes" || lc == "y" || lc == "1" {
		return DynamicValue{BoolValue: true, Type: TypeBool}
	} else if lc == "false" || lc == "no" || lc == "n" || lc == "0" {
		return DynamicValue{BoolValue: false, Type: TypeBool}
	}

	// Try integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return DynamicValue{IntValue: i, Type: TypeInt}
	}

	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return DynamicValue{FloatValue: f, Type: TypeFloat}
	}

	// Try common time formats
	timeFormats := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"01/02/2006",
		"01/02/2006 15:04:05",
	}

	if timeFormat != "" {
		timeFormats = append([]string{timeFormat}, timeFormats...)
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, s); err == nil {
			return DynamicValue{TimeValue: t, Type: TypeTime}
		}
	}

	// Default to string
	return DynamicValue{StringValue: s, Type: TypeString}
}

// ReadData reads records from the CSV file
func (s *CSVSource) ReadData(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record)
	errc := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errc)

		file, err := os.Open(s.Filename)
		if err != nil {
			errc <- fmt.Errorf("failed to open file: %v", err)
			return
		}
		defer file.Close()

		delimiter := s.Delimiter
		if delimiter == 0 {
			delimiter = ','
		}

		reader := csv.NewReader(file)
		reader.Comma = delimiter
		reader.LazyQuotes = true
		reader.TrimLeadingSpace = true

		// Read header or use column indices as names
		var header []string
		if s.HasHeader {
			header, err = reader.Read()
			if err != nil {
				errc <- fmt.Errorf("failed to read header: %v", err)
				return
			}
		}

		// Process each row
		for rowNum := 1; ; rowNum++ {
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			default:
				row, err := reader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					errc <- fmt.Errorf("error reading row %d: %v", rowNum, err)
					continue
				}

				// If this is the first row and we don't have a header, use indices
				if header == nil {
					header = make([]string, len(row))
					for i := range row {
						header[i] = fmt.Sprintf("column%d", i)
					}
				}

				record := NewRecord()
				for i, val := range row {
					if i >= len(header) {
						// Skip columns beyond the header length
						continue
					}

					columnName := header[i]
					if val == "" && s.SkipEmptyValues {
						continue
					}

					var typeHint ValueType
					var timeFormat string
					if s.TypeHints != nil {
						typeHint = s.TypeHints[columnName]
					}
					if s.TimeFormats != nil {
						timeFormat = s.TimeFormats[columnName]
					}

					record.Values[columnName] = newDynamicValueFromString(val, typeHint, timeFormat)
				}

				records <- record
			}
		}
	}()

	return records, errc
}

// CSVSink writes records to a CSV file
type CSVSink struct {
	Filename  string
	Delimiter rune
	Columns   []string // Optional column order specification
}

// WriteData writes records to the CSV file
func (s *CSVSink) WriteData(ctx context.Context, in <-chan Record) <-chan error {
	errc := make(chan error, 1)

	go func() {
		defer close(errc)

		file, err := os.Create(s.Filename)
		if err != nil {
			errc <- fmt.Errorf("failed to create file: %v", err)
			return
		}
		defer file.Close()

		delimiter := s.Delimiter
		if delimiter == 0 {
			delimiter = ','
		}

		writer := csv.NewWriter(file)
		writer.Comma = delimiter
		defer writer.Flush()

		// Determine columns to write
		var columns []string
		if len(s.Columns) > 0 {
			columns = s.Columns
		} else {
			// Read first record to determine columns
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			case record, ok := <-in:
				if !ok {
					// No records to write
					return
				}
				for column := range record.Values {
					columns = append(columns, column)
				}
				// Write the header
				if err := writer.Write(columns); err != nil {
					errc <- fmt.Errorf("failed to write header: %v", err)
					return
				}
				// Write the first record
				row := make([]string, len(columns))
				for i, column := range columns {
					if value, ok := record.Values[column]; ok {
						row[i] = value.String()
					}
				}
				if err := writer.Write(row); err != nil {
					errc <- fmt.Errorf("failed to write record: %v", err)
					return
				}
			}
		}

		// Process the rest of the records
		for {
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			case record, ok := <-in:
				if !ok {
					return
				}
				row := make([]string, len(columns))
				for i, column := range columns {
					if value, ok := record.Values[column]; ok {
						row[i] = value.String()
					}
				}
				if err := writer.Write(row); err != nil {
					errc <- fmt.Errorf("failed to write record: %v", err)
					continue
				}
			}
		}
	}()

	return errc
}

// FilterProcessor filters records based on a predicate
type FilterProcessor struct {
	Predicate func(Record) bool
}

// Process applies the filter to incoming records
func (p *FilterProcessor) Process(ctx context.Context, in <-chan Record) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for {
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			case record, ok := <-in:
				if !ok {
					return
				}
				if p.Predicate(record) {
					out <- record
				}
			}
		}
	}()

	return out, errc
}

// TransformProcessor transforms records
type TransformProcessor struct {
	Transform func(Record) Record
}

// Process applies the transformation to incoming records
func (p *TransformProcessor) Process(ctx context.Context, in <-chan Record) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for {
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			case record, ok := <-in:
				if !ok {
					return
				}
				out <- p.Transform(record)
			}
		}
	}()

	return out, errc
}

// Pipeline represents a data processing pipeline
type Pipeline struct {
	Source     DataSource
	Processors []DataProcessor
	Sink       DataSink
}

// Run executes the pipeline
func (p *Pipeline) Run(ctx context.Context) error {
	// Start with source
	data, errc1 := p.Source.ReadData(ctx)

	// Apply processors
	var errs []<-chan error
	errs = append(errs, errc1)

	// Apply each processor in sequence
	var processed <-chan Record = data
	for _, processor := range p.Processors {
		var errc <-chan error
		processed, errc = processor.Process(ctx, processed)
		errs = append(errs, errc)
	}

	// Send to sink
	errc2 := p.Sink.WriteData(ctx, processed)
	errs = append(errs, errc2)

	// Collect errors
	for _, errChan := range errs {
		for err := range errChan {
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	// Example usage
	ctx := context.Background()

	// Define optional type hints for specific columns
	typeHints := map[string]ValueType{
		"age":      TypeInt,
		"price":    TypeFloat,
		"active":   TypeBool,
		"birthday": TypeTime,
	}

	// Define optional time formats for time columns
	timeFormats := map[string]string{
		"birthday": "2006-01-02",
	}

	// Create a pipeline
	pipeline := &Pipeline{
		Source: &CSVSource{
			Filename:    "input.csv",
			HasHeader:   true,
			TypeHints:   typeHints,
			TimeFormats: timeFormats,
		},
		Processors: []DataProcessor{
			// Example: Filter out records where age < 18
			&FilterProcessor{
				Predicate: func(r Record) bool {
					age, ok := r.Values["age"]
					if !ok || age.Type != TypeInt {
						return true // Include if no age field or not an int
					}
					return age.IntValue >= 18
				},
			},
			// Example: Add a new calculated field
			&TransformProcessor{
				Transform: func(r Record) Record {
					// Make a copy of the record
					result := NewRecord()
					for k, v := range r.Values {
						result.Values[k] = v
					}

					// Add fullname field if first_name and last_name exist
					firstName, hasFirst := result.Values["first_name"]
					lastName, hasLast := result.Values["last_name"]
					if hasFirst && hasLast && firstName.Type == TypeString && lastName.Type == TypeString {
						fullName := firstName.StringValue + " " + lastName.StringValue
						result.Values["full_name"] = DynamicValue{
							StringValue: fullName,
							Type:        TypeString,
						}
					}
					return result
				},
			},
		},
		Sink: &CSVSink{
			Filename: "output.csv",
		},
	}

	// Run the pipeline
	if err := pipeline.Run(ctx); err != nil {
		log.Fatalf("Pipeline error: %v", err)
	}

	fmt.Println("Pipeline completed successfully")
}

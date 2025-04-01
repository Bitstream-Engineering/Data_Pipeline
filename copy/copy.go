package copy

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DataRecord represents a single record in our pipeline
type DataRecord struct {
	ID        int
	Timestamp time.Time
	Value     float64
	Category  string
	Metadata  map[string]string
}

// DataSource is an interface for data sources
type DataSource interface {
	ReadData(ctx context.Context) (<-chan DataRecord, error)
}

// DataSink is an interface for data sinks
type DataSink interface {
	WriteData(ctx context.Context, input <-chan DataRecord) error
}

// Processor is an interface for data processors
type Processor interface {
	Process(ctx context.Context, input <-chan DataRecord) <-chan DataRecord
}

// FileSource implements DataSource for reading from files
type FileSource struct {
	FilePath string
}

// ReadData reads data from a file and sends it to the returned channel
func (fs *FileSource) ReadData(ctx context.Context) (<-chan DataRecord, error) {
	file, err := os.Open(fs.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	output := make(chan DataRecord)

	go func() {
		defer file.Close()
		defer close(output)

		scanner := bufio.NewScanner(file)
		// Skip header
		if scanner.Scan() {
			// Header skipped
		}

		lineNum := 0
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				log.Println("File reading cancelled")
				return
			default:
				lineNum++
				line := scanner.Text()
				parts := strings.Split(line, ",")

				if len(parts) < 4 {
					log.Printf("Line %d: invalid format, skipping", lineNum)
					continue
				}

				id, err := strconv.Atoi(parts[0])
				if err != nil {
					log.Printf("Line %d: invalid ID, skipping: %v", lineNum, err)
					continue
				}

				timestamp, err := time.Parse(time.RFC3339, parts[1])
				if err != nil {
					log.Printf("Line %d: invalid timestamp, skipping: %v", lineNum, err)
					continue
				}

				value, err := strconv.ParseFloat(parts[2], 64)
				if err != nil {
					log.Printf("Line %d: invalid value, skipping: %v", lineNum, err)
					continue
				}

				metadata := make(map[string]string)
				for i := 4; i < len(parts); i += 2 {
					if i+1 < len(parts) {
						metadata[parts[i]] = parts[i+1]
					}
				}

				record := DataRecord{
					ID:        id,
					Timestamp: timestamp,
					Value:     value,
					Category:  parts[3],
					Metadata:  metadata,
				}

				output <- record
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading file: %v", err)
		}
	}()

	return output, nil
}

// ConsoleSink implements DataSink for writing to the console
type ConsoleSink struct{}

// WriteData writes data records to the console
func (cs *ConsoleSink) WriteData(ctx context.Context, input <-chan DataRecord) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-input:
			if !ok {
				return nil
			}
			fmt.Printf("ID: %d, Time: %s, Value: %.2f, Category: %s\n",
				record.ID, record.Timestamp.Format(time.RFC3339), record.Value, record.Category)
		}
	}
}

// FileSink implements DataSink for writing to files
type FileSink struct {
	FilePath string
}

// WriteData writes data records to a file
func (fs *FileSink) WriteData(ctx context.Context, input <-chan DataRecord) error {
	file, err := os.Create(fs.FilePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write header
	fmt.Fprintln(writer, "ID,Timestamp,Value,Category,MetadataKeys,MetadataValues")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-input:
			if !ok {
				return nil
			}

			// Serialize metadata
			var metadataKeys, metadataValues []string
			for k, v := range record.Metadata {
				metadataKeys = append(metadataKeys, k)
				metadataValues = append(metadataValues, v)
			}

			_, err := fmt.Fprintf(writer, "%d,%s,%.2f,%s,%s,%s\n",
				record.ID,
				record.Timestamp.Format(time.RFC3339),
				record.Value,
				record.Category,
				strings.Join(metadataKeys, "|"),
				strings.Join(metadataValues, "|"))

			if err != nil {
				return fmt.Errorf("failed to write record: %v", err)
			}
		}
	}
}

// FilterProcessor filters records based on a predicate
type FilterProcessor struct {
	Predicate func(DataRecord) bool
}

// Process filters records based on the predicate
func (fp *FilterProcessor) Process(ctx context.Context, input <-chan DataRecord) <-chan DataRecord {
	output := make(chan DataRecord)

	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case record, ok := <-input:
				if !ok {
					return
				}
				if fp.Predicate(record) {
					output <- record
				}
			}
		}
	}()

	return output
}

// TransformProcessor applies a transformation to each record
type TransformProcessor struct {
	Transform func(DataRecord) DataRecord
}

// Process transforms each record
func (tp *TransformProcessor) Process(ctx context.Context, input <-chan DataRecord) <-chan DataRecord {
	output := make(chan DataRecord)

	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case record, ok := <-input:
				if !ok {
					return
				}
				output <- tp.Transform(record)
			}
		}
	}()

	return output
}

// AggregateProcessor aggregates records by a key
type AggregateProcessor struct {
	KeyFunc       func(DataRecord) string
	AggregateFunc func([]DataRecord) DataRecord
	WindowSize    time.Duration
}

// Process aggregates records in time windows
func (ap *AggregateProcessor) Process(ctx context.Context, input <-chan DataRecord) <-chan DataRecord {
	output := make(chan DataRecord)

	go func() {
		defer close(output)

		windows := make(map[string][]DataRecord)
		ticker := time.NewTicker(ap.WindowSize)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Process and emit aggregated records
				for key, records := range windows {
					if len(records) > 0 {
						aggregated := ap.AggregateFunc(records)
						output <- aggregated
					}
					delete(windows, key)
				}
			case record, ok := <-input:
				if !ok {
					// Final processing
					for key, records := range windows {
						if len(records) > 0 {
							aggregated := ap.AggregateFunc(records)
							output <- aggregated
						}
						delete(windows, key)
					}
					return
				}

				key := ap.KeyFunc(record)
				windows[key] = append(windows[key], record)
			}
		}
	}()

	return output
}

// ParallelProcessor processes data in parallel
type ParallelProcessor struct {
	Workers       int
	ProcessorFunc func() Processor
}

// Process processes records in parallel
func (pp *ParallelProcessor) Process(ctx context.Context, input <-chan DataRecord) <-chan DataRecord {
	output := make(chan DataRecord)

	var wg sync.WaitGroup
	wg.Add(pp.Workers)

	// Fan out
	fanOut := make([]chan DataRecord, pp.Workers)
	for i := 0; i < pp.Workers; i++ {
		fanOut[i] = make(chan DataRecord)
		processor := pp.ProcessorFunc()

		go func(ch <-chan DataRecord, proc Processor) {
			defer wg.Done()

			// Process the data and forward to output
			resultCh := proc.Process(ctx, ch)
			for result := range resultCh {
				select {
				case <-ctx.Done():
					return
				case output <- result:
					// Result forwarded
				}
			}
		}(fanOut[i], processor)
	}

	// Distribute input
	go func() {
		defer func() {
			// Close all fan-out channels
			for _, ch := range fanOut {
				close(ch)
			}
		}()

		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case record, ok := <-input:
				if !ok {
					return
				}
				// Round-robin distribution
				select {
				case <-ctx.Done():
					return
				case fanOut[i] <- record:
					i = (i + 1) % pp.Workers
				}
			}
		}
	}()

	// Close output when all workers are done
	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

// Pipeline represents a data processing pipeline
type Pipeline struct {
	Source     DataSource
	Processors []Processor
	Sink       DataSink
}

// Run executes the pipeline
func (p *Pipeline) Run(ctx context.Context) error {
	// Start with the source
	dataChan, err := p.Source.ReadData(ctx)
	if err != nil {
		return fmt.Errorf("source error: %v", err)
	}

	// Apply each processor in sequence
	currentChan := dataChan
	for _, processor := range p.Processors {
		currentChan = processor.Process(ctx, currentChan)
	}

	// Send to sink
	return p.Sink.WriteData(ctx, currentChan)
}

func main() {
	// Create a context with cancelation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Example usage
	pipeline := &Pipeline{
		Source: &FileSource{
			FilePath: "input.csv",
		},
		Processors: []Processor{
			// Filter out negative values
			&FilterProcessor{
				Predicate: func(r DataRecord) bool {
					return r.Value >= 0
				},
			},
			// Transform values
			&TransformProcessor{
				Transform: func(r DataRecord) DataRecord {
					// Convert values to dollars
					r.Value = r.Value * 1.5
					return r
				},
			},
			// Aggregate by category in 5-second windows
			&AggregateProcessor{
				KeyFunc: func(r DataRecord) string {
					return r.Category
				},
				AggregateFunc: func(records []DataRecord) DataRecord {
					// Calculate average value
					var sum float64
					for _, r := range records {
						sum += r.Value
					}
					avg := sum / float64(len(records))

					// Use first record as base
					result := records[0]
					result.Value = avg
					result.Metadata["count"] = strconv.Itoa(len(records))
					result.Metadata["operation"] = "average"

					return result
				},
				WindowSize: 5 * time.Second,
			},
		},
		Sink: &FileSink{
			FilePath: "output_data.csv",
		},
	}

	// Log start time
	startTime := time.Now()
	log.Println("Starting pipeline...")

	// Run the pipeline
	if err := pipeline.Run(ctx); err != nil {
		log.Fatalf("Pipeline error: %v", err)
	}

	// Log completion
	log.Printf("Pipeline completed in %v", time.Since(startTime))
}

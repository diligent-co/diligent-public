package metrics

import (
	"coinbot/src/datamodels"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type FileFormat string

const (
	FormatCSV  FileFormat = "csv"
	FormatJSON FileFormat = "json"
)

// FileMetricsWriter writes metrics to local files in CSV or JSON format
type FileMetricsWriter struct {
	dateId     string
	baseDir    string
	files      map[string]*os.File
	csvWriters map[string]*csv.Writer
	fileFormat FileFormat
}

func NewFileMetricsWriter(baseDir string, format FileFormat) (*FileMetricsWriter, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metrics directory: %w", err)
	}
	now := time.Now()
	todaysDateId := fmt.Sprintf("%d%02d%02d", now.Year(), now.Month(), now.Day())

	return &FileMetricsWriter{
		dateId:     todaysDateId,
		baseDir:    baseDir,
		files:      make(map[string]*os.File),
		csvWriters: make(map[string]*csv.Writer),
		fileFormat: format,
	}, nil
}

func (w *FileMetricsWriter) Write(ctx context.Context, metric datamodels.Metric) error {
	extension := string(w.fileFormat)
	writerId := fmt.Sprintf("%s_%s", w.dateId, metric.MetricGeneratorName)
	filename := filepath.Join(w.baseDir, fmt.Sprintf("%s.%s", writerId, extension))
	// see if the file exists
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// Create new file for this source
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open metrics file: %w", err)
		}

		if w.fileFormat == FormatCSV {
			csvWriter := csv.NewWriter(f)
			w.csvWriters[writerId] = csvWriter

			// Get struct field names using reflection
			t := reflect.TypeOf(metric)
			var headers []string
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				jsonTag := field.Tag.Get("json")
				if jsonTag != "" {
					// Split the json tag to get just the name part
					tagParts := strings.Split(jsonTag, ",")
					headers = append(headers, tagParts[0])
				} else {
					headers = append(headers, field.Name)
				}
			}

			// Write CSV headers
			if err := csvWriter.Write(headers); err != nil {
				return fmt.Errorf("failed to write CSV headers: %w", err)
			}
			csvWriter.Flush()
		}
		w.files[writerId] = f
	}
	file := w.files[writerId]

	switch w.fileFormat {
	case FormatJSON:
		jsonBytes, err := json.Marshal(metric)
		if err != nil {
			return fmt.Errorf("failed to marshal metric to JSON: %w", err)
		}
		if _, err := file.Write(append(jsonBytes, '\n')); err != nil {
			return fmt.Errorf("failed to write JSON metrics: %w", err)
		}
	case FormatCSV:
		csvWriter := w.csvWriters[writerId]
		// Get values using reflection for CSV
		v := reflect.ValueOf(metric)
		var values []string
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			var value string
			switch val := field.Interface().(type) {
			case time.Time:
				value = val.Format(time.RFC3339)
			case []byte:
				// Try to convert JSON bytes to string representation
				var jsonVal interface{}
				if err := json.Unmarshal(val, &jsonVal); err == nil {
					jsonStr, err := json.Marshal(jsonVal)
					if err == nil {
						value = string(jsonStr)
					} else {
						value = string(val)
					}
				} else {
					value = string(val)
				}
			default:
				value = fmt.Sprintf("%v", val)
			}
			values = append(values, value)
		}

		if err := csvWriter.Write(values); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return fmt.Errorf("error flushing CSV writer: %w", err)
		}
	}

	return nil
}

func (w *FileMetricsWriter) Close() error {
	var lastErr error
	for source, file := range w.files {
		if w.fileFormat == FormatCSV {
			if writer := w.csvWriters[source]; writer != nil {
				writer.Flush()
				if err := writer.Error(); err != nil {
					slog.Error("Failed to flush CSV writer", "source", source, "error", err)
					lastErr = err
				}
			}
		}
		if err := file.Close(); err != nil {
			slog.Error("Failed to close metrics file", "source", source, "error", err)
			lastErr = err
		}
	}
	return lastErr
}
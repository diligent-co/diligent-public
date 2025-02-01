package metrics

import (
	"context"
	"log/slog"

	"coinbot/src/datamodels"
)

// MetricsWriter interface defines methods for writing metrics
type MetricsWriter interface {
	// Write takes any struct and writes it as metrics
	Write(ctx context.Context, metric datamodels.Metric) error
	// Close cleans up any resources
	Close() error
}

func BuildMetricsWriter(config *datamodels.MetricsWriterConfig) (MetricsWriter, error) {
	if config == nil {
		slog.Warn("MetricsWriterConfig is nil, skipping metrics writer")
		return nil, nil
	}
	writers := []MetricsWriter{}
	if config.WsWriter {
		writers = append(writers, NewWebSocketMetricsWriter())
	}
	if config.FileWriter {
		metricsWriter, err := NewFileMetricsWriter(config.FilePath, FormatCSV)
		if err != nil {
			return nil, err
		}
		writers = append(writers, metricsWriter)
	}
	return NewMultiMetricsWriter(writers...), nil
}

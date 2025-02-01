package metrics

import (
	"context"
	"sync"
	"log/slog"
	"math/rand"
	"coinbot/src/datamodels"
)

// MultiMetricsWriter writes metrics to multiple destinations
type MultiMetricsWriter struct {
	writers []MetricsWriter
	mu      sync.RWMutex
}

func NewMultiMetricsWriter(writers ...MetricsWriter) *MultiMetricsWriter {
	return &MultiMetricsWriter{
		writers: writers,
	}
}

func (w *MultiMetricsWriter) AddWriter(writer MetricsWriter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writers = append(w.writers, writer)
}

func (w *MultiMetricsWriter) Write(ctx context.Context, metric datamodels.Metric) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// log time of the metric if random var > 0.5
	if rand.Float64() > 0.8 {
		slog.Debug("MultiMetricsWriter writing metrics to writers for time", "time", metric.MetricTime)
	}

	var lastErr error
	for _, writer := range w.writers {
		if err := writer.Write(ctx, metric); err != nil {
			lastErr = err
			slog.Error("Failed to write metrics", 
				"writer", writer,
				"error", err)
		}
	}
	return lastErr
}

func (w *MultiMetricsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var lastErr error
	for _, writer := range w.writers {
		if err := writer.Close(); err != nil {
			lastErr = err
			slog.Error("Failed to close metrics writer",
				"writer", writer,
				"error", err)
		}
	}
	return lastErr
}
package metrics

import (
	"coinbot/src/database"
	"context"
	"coinbot/src/datamodels"
)

type DBMetricsWriter struct {
	db        database.MetricsDatabase
	tableName string
}

func NewDBMetricsWriter(db database.CoinbotDatabase) (*DBMetricsWriter, error) {
	// Create table if it doesn't exist
	return &DBMetricsWriter{
		db: db,
	}, nil
}

func (w *DBMetricsWriter) Write(ctx context.Context, metric datamodels.Metric) error {
	_, err := w.db.WriteNewMetric(ctx, metric)
	return err
}

func (w *DBMetricsWriter) Close() error {
	return nil
}

package database

import (
	"coinbot/src/datamodels"
	"context"

)

type MetricsDatabase interface {
	CreateNewMetricGenerator(ctx context.Context, metricGenerator datamodels.MetricGenerator) (int64, error)
	WriteNewMetric(ctx context.Context, metric datamodels.Metric) (int64, error)
}


func (a *databaseImplementation) CreateNewMetricGenerator(ctx context.Context, metricGenerator datamodels.MetricGenerator) (int64, error) {
	return a.gormDb.Create(&metricGenerator).RowsAffected, a.gormDb.Error
}

func (a *databaseImplementation) WriteNewMetric(ctx context.Context, metric datamodels.Metric) (int64, error) {
	return a.gormDb.Create(&metric).RowsAffected, a.gormDb.Error
}


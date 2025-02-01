package database

import (
	"coinbot/src/datamodels"
	"context"
	"time"

)

type AggFeedDatabase interface {
	WriteAggregatorValue(ctx context.Context, value datamodels.AggregatorValue) error
	WriteFeedValue(ctx context.Context, value datamodels.FeedValue) error
	GetAggregatorValues(ctx context.Context, aggregatorName string, asOfTime time.Time, limit int) ([]datamodels.AggregatorValue, error)
	GetFeedValues(ctx context.Context, feedName string, asOfTime time.Time, limit int) ([]datamodels.FeedValue, error)
}


func (a *databaseImplementation) WriteAggregatorValue(ctx context.Context, value datamodels.AggregatorValue) error {
	return a.gormDb.Create(&value).Error
}

func (a *databaseImplementation) WriteFeedValue(ctx context.Context, value datamodels.FeedValue) error {
	return a.gormDb.Create(&value).Error
}

func (a *databaseImplementation) GetAggregatorValues(ctx context.Context, aggregatorName string, asOfTime time.Time, limit int) ([]datamodels.AggregatorValue, error) {
	var values []datamodels.AggregatorValue
	err := a.gormDb.Where("aggregator_name = ? AND as_of_time <= ?", aggregatorName, asOfTime).Order("as_of_time DESC").Limit(limit).Find(&values).Error
	return values, err
}

func (a *databaseImplementation) GetFeedValues(ctx context.Context, feedName string, asOfTime time.Time, limit int) ([]datamodels.FeedValue, error) {
	var values []datamodels.FeedValue
	err := a.gormDb.Where("feed_name = ? AND timestamp <= ?", feedName, asOfTime).Order("timestamp DESC").Limit(limit).Find(&values).Error
	return values, err
}


package datamodels

import "time"

// gorm table of aggregator output
type AggregatorType string

const (
	HybridAggregator AggregatorType = "hybrid"
	TimedAggregator  AggregatorType = "timed"
)

type AggregatorValue struct {
	BaseModel
	AggregatorName   string           `gorm:"not null;index"`
	AsOfTime         time.Time        `gorm:"not null;index"`
	Value            DataPoint `gorm:"not null;type:jsonb"`
}

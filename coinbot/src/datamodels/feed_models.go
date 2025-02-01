package datamodels

import "time"

// gorm table of aggregator output
type FeedValue struct {
	BaseModel
	FeedName  string           `gorm:"not null;index"`
	Timestamp time.Time        `gorm:"not null;index"`
	FeedValue DataPoint `gorm:"not null;type:jsonb"`
}


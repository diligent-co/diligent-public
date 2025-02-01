package feeds

import (
	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/exchange"
	"coinbot/src/utils/errors"
	"context"
	"log/slog"
	"path/filepath"
	"time"
)

type DataFeed interface {
	// Subscribe returns a channel that will receive data points
	Subscribe(ctx context.Context, subscriberName string) (*datamodels.DataPointSubscription, error)
	Unsubscribe(ctx context.Context, subscriptionId string) error
	// GetName returns the name of the feed
	GetName() string
	GetOutputFieldNames() []string
	Start(ctx context.Context) error
	IsStarted() bool
	Stop() error
}

var DefaultPartialCsvFeedConfig = datamodels.FeedConfig{
	FeedName:       "",
	TickInterval:   time.Minute,
	InterReadDelay: time.Millisecond * 20,
	StartTime:      "",
	EndTime:        "",
	IsLive:         false,
	CsvFeedConfig: &datamodels.CsvFeedConfig{
		FilePath:         "",
		RelationName:     "",
		Asset:            "",
		TimestampColName: "timestamp",
		Columns: []datamodels.CsvColumnConfig{
			{
				ColumnIndex: 0,
				FieldName:   "timestamp",
				FieldType:   datamodels.FieldTypeInt,
			},
			{
				ColumnIndex: 1,
				FieldName:   "price",
				FieldType:   datamodels.FieldTypeFloat,
			},
			{
				ColumnIndex: 2,
				FieldName:   "volume",
				FieldType:   datamodels.FieldTypeFloat,
			},
		},
	},
	LiveFeedConfig: nil,
}

func NewDataFeedFromConfig(config *datamodels.FeedConfig, db database.AggFeedDatabase, exchangeClient exchange.ExchangeClient) (DataFeed, error) {
	switch config.IsLive {
	case true:
		feed, err := liveKrakenFeedFromConfig(config.LiveFeedConfig, exchangeClient, db)
		return feed, err
	case false:
		csvSchema, err := csvSchemaFromConfig(config.CsvFeedConfig)
		if err != nil {
			return nil, err
		}
		if config.CsvFeedConfig.FilePath == "" {
			return nil, errors.New("csv feed config file path is required")
		} else {
			slog.Info("Using CSV feed", "path", config.CsvFeedConfig.FilePath)
		}

		startTime, err := time.Parse(time.RFC3339, config.StartTime)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse start time")
		}
		endTime, err := time.Parse(time.RFC3339, config.EndTime)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse end time")
		}
		csvFeed, err := NewCsvFeedBuilder(config.CsvFeedConfig.FilePath).
			WithStartTime(startTime).
			WithEndTime(endTime).
			WithSchema(csvSchema).
			Build()
		return csvFeed, err
	default:
		return nil, errors.New("invalid feed config")
	}

}

func liveKrakenFeedFromConfig(config *datamodels.LiveFeedConfig, exchangeClient exchange.ExchangeClient, db database.AggFeedDatabase) (DataFeed, error) {
	pair := string(config.Asset) + "/USD"
	feed := NewKrakenExchangeFeed(exchangeClient,
		datamodels.KrakenDataChannelTrade,
		pair,
		config.WriteToDatabase,
		db)
	return feed, nil
}

func csvSchemaFromConfig(config *datamodels.CsvFeedConfig) (*datamodels.CsvWithTimestampSchema, error) {
	csvWithTimestampSchema := datamodels.CsvWithTimestampSchema{
		RelationName:       config.RelationName,
		TimestampFieldName: config.TimestampColName,
	}
	for _, col := range config.Columns {
		csvWithTimestampSchema.Fields = append(csvWithTimestampSchema.Fields, datamodels.CsvField{
			ColumnIndex:  col.ColumnIndex,
			FieldName:    col.FieldName,
			FieldType:    col.FieldType,
			DefaultValue: nil,
		})
	}
	return &csvWithTimestampSchema, nil
}

func FillPartialFeedConfig(feedName string,
	asset datamodels.Asset,
	startTime string,
	endTime string,
	dataDir string) (*datamodels.FeedConfig, error) {

	partialFeedConfig := DefaultPartialCsvFeedConfig
	partialFeedConfig.FeedName = feedName
	partialFeedConfig.CsvFeedConfig.Asset = asset
	partialFeedConfig.StartTime = startTime
	partialFeedConfig.EndTime = endTime

	partialFeedConfig.CsvFeedConfig.RelationName = feedName
	filePath := filepath.Join(dataDir, string(asset)+"USD"+".csv")
	partialFeedConfig.CsvFeedConfig.FilePath = filePath

	slog.Info("Using default partial feed config", "feed_name", feedName, "asset", asset, "file_path", filePath)
	return &partialFeedConfig, nil
}

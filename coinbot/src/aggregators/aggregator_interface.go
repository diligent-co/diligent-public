package aggregators

import (
	"context"
	"time"

	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
	"coinbot/src/utils/errors"
)

type Aggregator interface {
	GetName() string
	Start(ctx context.Context) error // reads from feeds
	IsStarted() bool
	Stop() error                           // stops the aggregator
	GetDataPoints() []datamodels.DataPoint // returns all the data points currently in the aggregator
	GetTotalInboundDataPointCount() int
	GetLastCalculatedValue() datamodels.DataPoint
	// subscribing strategies
	Subscribe(ctx context.Context, strategyId string) (*datamodels.DataPointSubscription, error)
	Unsubscribe(ctx context.Context, strategyId string) error
	GetSubscribers() map[string]*datamodels.DataPointSubscription
	GetInputFieldNames() []string
	GetOutputFieldNames() []string
}

func BuildAggregatorFromConfig(config *datamodels.AggregatorConfig, db database.AggFeedDatabase, feed feeds.DataFeed) (Aggregator, error) {
	aggregatorFunc, err := getAggregatorFunc(config.AggregatorFunc)
	if err != nil {
		return nil, err
	}
	switch config.AggregatorType {
	case datamodels.TimedAggregator:
		aggregator := NewTimedAggregator(db, 
			feed, 
			time.Second*time.Duration(config.WindowSize),
			time.Second*time.Duration(config.EmitCadence),
			aggregatorFunc, 
			config.InputFields, 
			config.OutputFields)
		return aggregator, nil
	case datamodels.HybridAggregator:
		aggregator, err := NewHybridAggregator(db, 
			feed, 
			time.Second*time.Duration(config.WindowSize),
			time.Second*time.Duration(config.EmitCadence),
			aggregatorFunc, 
			config.InputFields, 
			config.OutputFields)
		if err != nil {
			return nil, err
		}
		return aggregator, nil
	default:
		return nil, errors.New("invalid aggregator type")
	}
}


func getAggregatorFunc(funcName string) (AggregatorFunction, error) {
	switch funcName {
	case "mean":
		return MeanFunc, nil
	case "last":
		return LastValueFunc, nil
	case "last_f1_divided_by_f2":
		return LastValueF1DividedByF2Func, nil
	case "last_f1_minus_f2_divided_by_f3":
		return LastValueF1MinusF2DividedByF3Func, nil
	case "max":
		return MaxFunc, nil
	case "min":
		return MinFunc, nil
	case "variance":
		return VarianceFunc, nil
	case "stddev":
		return StandardDeviationFunc, nil
	case "median":
		return MedianFunc, nil
	case "lin_reg_slope":
		return LinRegSlopeFunc, nil
	case "vwap":
		return VWAPFunc, nil
	default:
		return nil, errors.New("invalid aggregator function")
	}
}

var PartialAggregatorConfig = datamodels.AggregatorConfig{
	Name: "",
	AggregatorType: datamodels.TimedAggregator,
	InputFeedName: "",
	AggregatorFunc: "last",
	InputFields: []string{"price"},
	OutputFields: []string{"price"},
	WindowSize: 1,
	EmitCadence: 1,
}


func FillPartialAggregatorConfig(name string, inputFeedName string) datamodels.AggregatorConfig {
	partialAggregatorConfig := PartialAggregatorConfig
	partialAggregatorConfig.Name = name
	partialAggregatorConfig.InputFeedName = inputFeedName
	return partialAggregatorConfig
}
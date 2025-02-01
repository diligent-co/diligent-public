package strategies

import (
	"context"
	"errors"
	"fmt"
	"time"

	"coinbot/src/aggregators"
	"coinbot/src/datamodels"
	"coinbot/src/metrics"
)

type StrategyEngine interface {
	GetName() string
	SetName(name string)
	Start(ctx context.Context) error
	Stop() error
	GetDataStartTime() time.Time
	IsStarted() bool
	// Subcribing to a strategy will create a subscription to the signals
	// that the strategy emits
	Subscribe(ctx context.Context, subscriberName string) (StrategySubscription, error)
	Unsubscribe(ctx context.Context, subscriberId string) error
}

func StrategyEngineFromConfig(config *datamodels.StrategyConfig,
	aggregators map[string]aggregators.Aggregator) (StrategyEngine, error) {

	metricsWriter, err := metrics.BuildMetricsWriter(config.MetricsWriter)
	if err != nil {
		return nil, err
	}

	signalFuncSupplier, err := buildSignalFunctionSupplierFromConfig(config.SignalFunc)
	if err != nil {
		return nil, err
	}

	strategyEngine := NewStratEngine().
		WithSignalFunctionSupplier(signalFuncSupplier).
		WithAggregators(aggregators).
		WithRealTime(config.IsRealTime).
		WithStepCadence(config.StepCadence)

	if metricsWriter != nil {
		strategyEngine.WithMetricsWriter(metricsWriter)
	}

	builtStrategyEngine, err := strategyEngine.Build()
	if err != nil {
		return nil, err
	}

	return builtStrategyEngine, nil

}

func buildSignalFunctionSupplierFromConfig(config *datamodels.SignalFunctionConfig) (SignalFunctionSupplier, error) {
	switch config.Type {
	case "momentum":
		assets := make([]datamodels.Asset, len(config.Assets))
		for i, asset := range config.Assets {
			assets[i] = datamodels.Asset(asset)
		}
		return GetSimpleMomentumSignalSupplier(assets), nil
	case "external":
		if config.ExternalSupplier == nil {
			return nil, errors.New("external supplier config is required for external strategy type")
		}
		return NewExternalSignalSupplier(config.ExternalSupplier, config.Assets), nil
	default:
		return nil, errors.New(fmt.Sprintf("unknown strategy type: %s", config.Type))
	}
}

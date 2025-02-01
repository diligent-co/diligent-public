//go:build unit

package portfolio

import (
	"coinbot/src/aggregators"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
	"coinbot/src/strategies"
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func signalFunc(aggregatorBuffer map[string][]datamodels.DataPoint) *datamodels.Signal {
	// random buy signal
	return &datamodels.Signal{
		SignalType: datamodels.SignalTypeAllocation,
		Allocations: map[datamodels.Asset]float64{
			datamodels.XBT: 1.0,
		},
	}
}

func TestSimulatedPortfolio(t *testing.T) {
	ctx := context.Background()
	pairV1 := "XBTUSD"
	schema := GetSchema(pairV1)

	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)

	btcUsdCsvPath := thisDir + "/../../test/data/" + pairV1 + ".csv"

	feed, err := feeds.NewCsvFeedBuilder(btcUsdCsvPath).
		WithStartTime(time.Date(2021, 6, 17, 15, 33, 0, 0, time.UTC)).
		WithSchema(schema).
		Build()
	if err != nil {
		t.Fatalf("failed to create kraken csv feed: %v", err)
	}
	fc := feeds.NewFeedCoordinator(ctx, time.Minute)
	fc.WithInterReadDelay(time.Millisecond *50)
	fc.AddFeed(feed)

	shortMovingAverageAggregator, err := aggregators.NewHybridAggregator(
		nil,
		feed,
		1*time.Minute,
		30*time.Second,
		aggregators.MeanFunc,
		[]string{"price"},
		[]string{"mean_price"})
	if err != nil {
		t.Fatalf("failed to create short moving average aggregator: %v", err)
	}

	longMovingAverageAggregator, err := aggregators.NewHybridAggregator(
		nil,
		feed,
		5*time.Minute,
		30*time.Second,
		aggregators.MeanFunc,
		[]string{"price"},
		[]string{"mean_price"})
	if err != nil {
		t.Fatalf("failed to create long moving average aggregator: %v", err)
	}

	lastPriceAggregator := aggregators.NewTimedAggregator(
		nil,
		feed,
		0,
		100*time.Millisecond,
		aggregators.LastValueFunc,
		[]string{"price"},
		[]string{""})

	strategyEngine, buildErr := strategies.NewStratEngine().
		WithAggregator("short_ma", shortMovingAverageAggregator).
		WithAggregator("long_ma", longMovingAverageAggregator).
		WithSignalFunctionSupplier(strategies.GetSingleAssetDummySignalSupplier([]string{"XBTUSD"})).
		WithRealTime(false).
		WithStepCadence(10 * time.Second).
		Build()
	assert.NotNil(t, strategyEngine)
	assert.NoError(t, buildErr)

	// portfolio
	portfolio, buildErr := NewSimulatedPortfolio().
		WithStrategies([]strategies.StrategyEngine{strategyEngine}).
		WithInitialBalance(10000).
		WithPriceAggregators(map[datamodels.Asset]aggregators.Aggregator{datamodels.XBT: lastPriceAggregator}).//, datamodels.USD: lastPriceAggregator}).
		WithAssets([]datamodels.Asset{datamodels.XBT}).//, datamodels.USD}).
		Build()
	assert.NotNil(t, portfolio)
	assert.NoError(t, buildErr)

	portfolioStartErr := portfolio.Start(ctx)
	assert.NoError(t, portfolioStartErr)
	defer portfolio.Stop()

	metricsCh := portfolio.SubscribeToMetrics()

	// Create a timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Create a slice to store metrics
	var metrics []datamodels.Metric

	// Listen for metrics with timeout
	//done := make(chan bool)
	func() {
		for {
			select {
			case metric, ok := <-metricsCh:
				if !ok {
					//done <- true
					return
				}
				metrics = append(metrics, metric)
			case <-timeoutCtx.Done():
				//done <- true
				return
			}
		}
	}()

	// Wait for collection to finish
	//<-done

	// Add assertions
	if len(metrics) == 0 {
		t.Fatal("no metrics received")
	}

	// Test that portfolio value changes
	firstMetric := metrics[0]
	portfolioMetrics := datamodels.PortfolioRealTimeMetrics{}
	json.Unmarshal(firstMetric.MetricValue, &portfolioMetrics)
	initialValue := portfolioMetrics.TotalValue

	lastMetric := metrics[len(metrics)-1]
	json.Unmarshal(lastMetric.MetricValue, &portfolioMetrics)
	finalValue := portfolioMetrics.TotalValue
	if initialValue == finalValue {
		t.Error("portfolio value did not change")
	}

	// Test that trades were executed
	transactions := portfolio.GetTransactions()
	if len(transactions) == 0 {
		t.Error("no trades were executed")
	}

	// make sure all aggregators processed the same number of datapoints
	shortTotal := shortMovingAverageAggregator.GetTotalInboundDataPointCount()
	longTotal := longMovingAverageAggregator.GetTotalInboundDataPointCount()
	lastTotal := lastPriceAggregator.GetTotalInboundDataPointCount()
	if shortTotal != longTotal || shortTotal != lastTotal {
		t.Error("aggregators processed different number of datapoints", "short", shortTotal, "long", longTotal, "last", lastTotal)
	}

	// should execute 413 trades
	if len(transactions) < 30 {
		t.Errorf("expected at least 30 trades, got %d", len(transactions))
	}

	t.Logf("Received %d metrics updates", len(metrics))
	t.Logf("Executed %d trades", len(transactions))
	t.Logf("Portfolio value changed from %.2f to %.2f", initialValue, finalValue)

}

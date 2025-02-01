package portfolio

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"coinbot/src/aggregators"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
	"coinbot/src/strategies"
)

func GetSchema(relationName string) *datamodels.CsvWithTimestampSchema {
	return &datamodels.CsvWithTimestampSchema{
		RelationName:       "",
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt, DefaultValue: 0},
			{ColumnIndex: 1, FieldName: "price", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
			{ColumnIndex: 2, FieldName: "volume", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
		},
	}
}

func createFeatureAggregators(feed feeds.DataFeed) (map[string]aggregators.Aggregator, error) {
	output := map[string]aggregators.Aggregator{}
	shortAgg, err :=  aggregators.NewHybridAggregator(
			nil, feed, time.Second*30, 0,
			aggregators.MeanFunc, []string{"price"}, []string{"ma_short"})
	if err != nil {
		return nil, err
	}
	output["ma_short"] = shortAgg
	longAgg, err := aggregators.NewHybridAggregator(
			nil, feed, time.Minute*2, 0,
			aggregators.MeanFunc, []string{"price"}, []string{"ma_long"})
	if err != nil {
		return nil, err
	}
	output["ma_long"] = longAgg
	volatilityAgg, err := aggregators.NewHybridAggregator(
			nil, feed, time.Minute*2, 0,
			aggregators.StandardDeviationFunc, []string{"price"}, []string{"volatility"})
	if err != nil {
		return nil, err
	}
	output["volatility"] = volatilityAgg
	return output, nil
}

func createPriceAggregator(feed feeds.DataFeed) (aggregators.Aggregator, error) {
		priceAgg,err := aggregators.NewHybridAggregator(nil, feed, 0,
			100*time.Millisecond, //! This update cadence isn't allowed, it must be at least a minute. Are we mixing up streamtime and realtime?
			aggregators.LastValueFunc,
			[]string{"price"},
			[]string{"price"})
	if err != nil {
		return nil, err
	}
	return priceAgg, nil
}

func TestMultiAssetPortfolio(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	nonUsdAssets := []datamodels.Asset{datamodels.XBT, datamodels.ETH, datamodels.SOL}

	dataFeeds := make(map[datamodels.Asset]feeds.DataFeed)
	featureAggregators := make(map[datamodels.Asset]map[string]aggregators.Aggregator)
	priceAggregators := make(map[datamodels.Asset]aggregators.Aggregator)

	for _, asset := range nonUsdAssets {
		schema := GetSchema(string(asset))
		csvPath := thisDir + "/../../test/data/" + string(asset)+"USD" + ".csv"
		feed, err := feeds.NewCsvFeedBuilder(csvPath).
			WithStartTime(time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)).
			WithSchema(schema).
			WithHasHeader(false).
			Build()
		if err != nil {
			t.Fatalf("failed to create feed for %s: %v", asset, err)
		}
		fc := feeds.NewFeedCoordinator(ctx, time.Minute)
		fc.AddFeed(feed)
		dataFeeds[asset] = feed
		featureAggregators[asset], err = createFeatureAggregators(feed)
		if err != nil {
			t.Fatalf("failed to create feature aggregators for %s: %v", asset, err)
		}
		priceAggregators[asset], err = createPriceAggregator(feed)
		if err != nil {
			t.Fatalf("failed to create price aggregator for %s: %v", asset, err)
		}
	}

	// Create strategy with all aggregators
	pairs := []string{}
	for _, asset := range nonUsdAssets {
		pairs = append(pairs, string(asset)+"USD")
	}
	strategy := strategies.NewStratEngine().
		WithSignalFunctionSupplier(strategies.GetDummySignalSupplier(pairs)).
		WithRealTime(false).
		WithStepCadence(5 * time.Minute)

	// Add all aggregators to strategy
	for asset, aggs := range featureAggregators {
		for name, agg := range aggs {
			strategy.WithAggregator(string(asset)+"_"+name, agg)
		}
	}

	strategy, buildErr := strategy.Build()
	assert.NotNil(t, strategy)
	assert.NoError(t, buildErr)

	// Create and start portfolio
	portfolio, buildErr := NewSimulatedPortfolio().
		WithStrategies([]strategies.StrategyEngine{strategy}).
		WithAssets(nonUsdAssets).
		WithPriceAggregators(priceAggregators).
		//WithPriceAggregator("USD", priceAggregators[datamodels.USD]).
		//WithPriceAggregator("XBTUSD", priceAggregators["XBTUSD"]).
		//WithPriceAggregator("ETHUSD", priceAggregators["ETHUSD"]).
		//WithPriceAggregator("SOLUSD", priceAggregators["SOLUSD"]).
		WithInitialBalance(100000).
		Build()
	assert.NotNil(t, portfolio)
	assert.NoError(t, buildErr)

	metricsSubscription := portfolio.SubscribeToMetrics()
	done := make(chan bool)
	if err := portfolio.Start(ctx); err != nil {
		t.Fatalf("failed to start portfolio: %v", err)
	}

	go func() {
		lastUpdate := time.Now()
		timeout := time.NewTimer(2 * time.Second)

		for {
			select {
			case <-done:
				return
			case metric, ok := <-metricsSubscription:
				if !ok {
					return
				}
				lastUpdate = time.Now()
				// Optional: log metrics as they come in
				portfolioMetrics := datamodels.PortfolioRealTimeMetrics{}
				json.Unmarshal(metric.MetricValue, &portfolioMetrics)
				t.Logf("Received metric update: value=$%.2f", portfolioMetrics.TotalValue)

			case <-timeout.C:
				// If we haven't received updates for 2 seconds, assume we're done
				if time.Since(lastUpdate) > 2*time.Second {
					t.Logf("Timeout waiting for portfolio processing")
					return
				}
				timeout.Reset(2 * time.Second)

			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for processing to complete or context to timeout
	select {
	case <-done:
		// Processing completed normally
	case <-ctx.Done():
		t.Fatal("test timed out waiting for portfolio processing")
	}

	if err := portfolio.Stop(); err != nil {
		t.Fatalf("failed to stop portfolio: %v", err)
	}

	metrics := portfolio.metrics
	if len(metrics) == 0 {
		t.Fatal("no metrics collected")
	}

	// Log performance metrics
	t.Logf("Final portfolio value: $%.2f", metrics[len(metrics)-1].TotalValue)

	// Verify allocations sum to approximately 1
	lastMetric := metrics[len(metrics)-1]
	totalAllocation := 0.0
	for _, pos := range lastMetric.Positions {
		totalAllocation += pos.Amount * pos.AvgPrice / lastMetric.TotalValue
	}
	if totalAllocation < 0.95 || totalAllocation > 1.05 {
		t.Errorf("Total allocation (%.2f) not close to 1.0", totalAllocation)
	}
}

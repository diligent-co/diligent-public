package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"time"
	"log/slog"

	"coinbot/src/aggregators"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
	"coinbot/src/metrics"
	"coinbot/src/portfolio"
	"coinbot/src/strategies"
	"coinbot/src/utils/errors"
)

func initializeBacktest(ctx context.Context, metricsWriter metrics.MetricsWriter) {

	// _ = "BTC/USD"
	pairV1 := "XBTUSD"
	schema := datamodels.CsvWithTimestampSchema{
		RelationName:       pairV1,
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
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
	}
	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)

	btcUsdCsvPath := thisDir + "/../../data/Kraken_Trading_History/" + pairV1 + ".csv"
	feed, err := feeds.NewCsvFeedBuilder(btcUsdCsvPath).
		WithTickInterval(time.Minute).
		WithStartTime(time.Date(2021, 6, 17, 15, 33, 0, 0, time.UTC)).
		WithEndTime(time.Date(2021, 6, 27, 15, 33, 0, 0, time.UTC)).
		WithSchema(&schema).
		WithInterReadDelay(time.Millisecond * 20). // low values clog up the system (10ms is too fast)
		Build()
	if err != nil {
		slog.Error("Failed to create CSV feed", "error", err)
		os.Exit(1)
	}
	if feed == nil {
		slog.Error("Feed is nil")
		os.Exit(1)
	}

	xbtMaLongAggregator := aggregators.NewHybridAggregator(
		nil,
		feed,
		time.Minute*5,
		time.Minute* 1,
		aggregators.MeanFunc,
		[]string{"price"},
		[]string{"ma_long"})

	xbtMaShortAggregator := aggregators.NewHybridAggregator(
		nil,
		feed,
		time.Second*60,
		time.Minute* 1,
		aggregators.MeanFunc,
		[]string{"price"},
		[]string{"ma_short"})
	
	xbtVolatilityAggregator := aggregators.NewHybridAggregator(
		nil,
		feed,
		time.Minute*5,
		time.Minute*1,
		aggregators.StandardDeviationFunc,
		[]string{"price"},
		[]string{"volatility"})

	xbtPriceAggregator := aggregators.NewTimedAggregator(
		nil,
		feed,
		0,
		100*time.Millisecond,
		aggregators.LastValueFunc,
		[]string{"price"},
		[]string{"price"})

	usdAggregator := aggregators.NewTimedAggregator(
		nil,
		feed,
		0,
		100*time.Millisecond,
		aggregators.CreateConstantValueAggregatorFunc([]string{"price"}, []string{"price"}, 1.0),
		[]string{"price"},
		[]string{"price"})
	
	usdMaLongAggregator := aggregators.NewHybridAggregator(
		nil,
		usdAggregator,
		time.Second*60,
		time.Minute* 1,
		aggregators.MeanFunc,
		[]string{"price"},
		[]string{"ma_long"})

	usdMaShortAggregator := aggregators.NewHybridAggregator(
		nil,
		usdAggregator,
		time.Second*30,
		time.Minute* 1,
		aggregators.MeanFunc,
		[]string{"price"},
		[]string{"ma_short"})

	usdVolatilityAggregator := aggregators.NewHybridAggregator(
		nil,
		usdAggregator,
		time.Minute*1,
		time.Minute*1,
		aggregators.StandardDeviationFunc,
		[]string{"price"},
		[]string{"volatility"})

	signalSupplier := strategies.GetSimpleMomentumSignalSupplier([]datamodels.Asset{datamodels.XBT, datamodels.USD})

	strategyEngine, buildErr := strategies.NewStratEngine().
		WithAggregator("XBT_ma_long", xbtMaLongAggregator).
		WithAggregator("XBT_ma_short", xbtMaShortAggregator).
		WithAggregator("XBT_volatility", xbtVolatilityAggregator).
		WithAggregator("USD_ma_long", usdMaLongAggregator).
		WithAggregator("USD_ma_short", usdMaShortAggregator).
		WithAggregator("USD_volatility", usdVolatilityAggregator).
		WithSignalFunctionSupplier(signalSupplier).
		WithMetricsWriter(metricsWriter).
		WithRealTime(false).
		WithStepCadence(time.Minute).
		Build()

	if buildErr != nil {
		errors.Wrap(buildErr, "failed to build strategy engine")
		os.Exit(1)
	}

	// portfolio
	portfolio, buildErr := portfolio.NewSimulatedPortfolio().
		WithStrategies([]strategies.StrategyEngine{strategyEngine}).
		WithInitialBalance(10000).
		WithPriceAggregator("XBT", xbtPriceAggregator).
		WithPriceAggregator("USD", usdAggregator).
		WithAssets([]datamodels.Asset{datamodels.XBT, datamodels.USD}).
		WithMetricsWriter(metricsWriter).
		Build()
	if buildErr != nil {
		errors.Wrap(buildErr, "failed to build portfolio")
		os.Exit(1)
	}

	portfolioMetricsSubscription := portfolio.SubscribeToMetrics()

	portfolioStartErr := portfolio.Start(ctx)
	if portfolioStartErr != nil {
		errors.Wrap(portfolioStartErr, "failed to start portfolio")
	}

	// listen to metrics chan until it's complete or ctx is cancelled
	metricsPlotter := metrics.NewMetricPlotter().
		WithAppOutput().
		Build()
		go updatePlotterData(ctx, metricsPlotter, portfolioMetricsSubscription)
	metricsPlotter.Plot()

}


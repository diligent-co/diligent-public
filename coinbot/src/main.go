package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"coinbot/src/config"
	"coinbot/src/server"
)

func main() {
	initializeLogging()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coinbotConfig, err := config.Load()
	if err != nil {
		slog.Error("Failed to load config", "error", err)
	}
	slog.Info("Ramping up Coinbot")

	// symbolDict := symbols.NewSymbolsDictionaryFromConfig(&coinbotConfig.SymbolsConfig)

	_ = coinbotConfig


	// Create and configure the server
	srv := server.NewServer(":8080")
	if srv == nil {
		slog.Error("Server is nil")
		os.Exit(1)
	}

	// Start the server
	go func() {
		slog.Info("Starting server")
		if err := srv.Start(ctx); err != nil {
			slog.Error("Server failed", "error", err)
		}
	}()

	//go server.StartHeartbeat(ctx)	

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down...")
}

func initializeLogging() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}
	switch strings.ToLower(logLevel) {
	case "debug":
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})))
	case "info":
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelInfo})))
	default:
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelInfo})))
	}
}





// func initializeRealTimeFeed(ctx context.Context, coinbotConfig *config.CoinbotConfig, pairV1 string) {

// 	// Connect to database
// 	db, err := database.NewDBConnection(coinbotConfig.DatabaseConfig)
// 	if err != nil {
// 		slog.Error("Failed to connect to database", "error", err)
// 	}

	// // Connect to Exchange
	// krakenClient := exchange.NewKrakenClient(coinbotConfig.KrakenConfig)
	// if err := krakenClient.Connect(ctx); err != nil {
	// 	errors.Wrap(err, "failed to connect to kraken")
	// }
	// defer krakenClient.Close()

// 	realTimeFeed := feeds.NewKrakenExchangeFeed(krakenClient,
// 		datamodels.KrakenDataChannelTrade,
// 		"BTC/USD",
// 		true,
// 		db)
// 	thirtySecMovingAverageAggregator := aggregators.NewHybridAggregator(
// 		db,
// 		realTimeFeed,
// 		time.Second*30,
// 		0,
// 		aggregators.MeanFunc,
// 		[]string{"price"},
// 		[]string{"ma_short"})

// 	tenSecMovingAverageAggregator := aggregators.NewHybridAggregator(
// 		db,
// 		realTimeFeed,
// 		time.Second*10,
// 		0,
// 		aggregators.MeanFunc,
// 		[]string{"price"},
// 		[]string{"ma_long"})

// 	lastPriceAggregator := aggregators.NewTimedAggregator(
// 		db,
// 		realTimeFeed,
// 		0,
// 		100*time.Millisecond,
// 		aggregators.LastValueFunc,
// 		[]string{"price"},
// 		[]string{"last_price"})

// 	strategyEngine, buildErr := strategies.NewStratEngine().
// 		WithAggregator("30s_ma", thirtySecMovingAverageAggregator).
// 		WithAggregator("10s_ma", tenSecMovingAverageAggregator).
// 		WithAggregator("last_price", lastPriceAggregator).
// 		Build()
// 	if buildErr != nil {
// 		errors.Wrap(buildErr, "failed to build strategy engine")
// 		os.Exit(1)
// 	}

// 	strategyStartErr := strategyEngine.Start(ctx)
// 	if strategyStartErr != nil {
// 		errors.Wrap(strategyStartErr, "failed to start strategy engine")
// 	}

// 	// portfolio
// 	portfolio, buildErr := portfolio.NewSimulatedPortfolio().
// 		WithStrategies([]strategies.StrategyEngine{strategyEngine}).
// 		WithInitialBalance(10000).
// 		WithPriceAggregator(pairV1, lastPriceAggregator).
// 		WithAssets([]string{pairV1}).
// 		Build()
// 	if buildErr != nil {
// 		errors.Wrap(buildErr, "failed to build portfolio")
// 		os.Exit(1)
// 	}

// 	portfolioStartErr := portfolio.Start(ctx)
// 	if portfolioStartErr != nil {
// 		errors.Wrap(portfolioStartErr, "failed to start portfolio")
// 	}
// 	defer portfolio.Stop()
// }

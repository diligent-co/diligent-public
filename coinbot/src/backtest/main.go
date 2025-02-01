package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"os/signal"
	"syscall"

	"coinbot/src/datamodels"
	"coinbot/src/metrics"
	"coinbot/src/portfolio"
	"coinbot/src/utils/errors"
)

// NewDefaultPortfolioConfig returns a configuration with sensible defaults
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, thisFile, _, _ := runtime.Caller(0)
	baseDir := filepath.Dir(thisFile)

	// get config filepath from first arg
	if len(os.Args) < 2 {
		slog.Error("No config file provided")
		os.Exit(1)
	}
	configFilePath := os.Args[1]

	slog.Info("Using config file", "path", configFilePath)

	portfolioConfig, err := datamodels.NewPortfolioConfig(configFilePath, baseDir)
	if err != nil {
		slog.Error("Failed to create portfolio config", "error", err)
		os.Exit(1)
	}

	if err := portfolioConfig.Validate(); err != nil {
		slog.Error("Invalid portfolio config", "error", err)
		os.Exit(1)
	}

	portfolio, err := portfolio.BuildPortfolioFromConfig(ctx, portfolioConfig, nil, nil)
	if err != nil {
		slog.Error("Failed to create portfolio", "error", err)
		os.Exit(1)
	}

	//portfolioMetricsSubscription := portfolio.SubscribeToMetrics()

	portfolioStartErr := portfolio.Start(ctx)
	if portfolioStartErr != nil {
		errors.Wrap(portfolioStartErr, "failed to start portfolio")
		os.Exit(1)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down...")


	// listen to metrics chan until it's complete or ctx is cancelled
	// metricsPlotter := metrics.NewMetricPlotter().
	// 	WithAppOutput().
	// 	Build()
	// go updatePlotterData(ctx, metricsPlotter, portfolioMetricsSubscription)
	// metricsPlotter.Plot()

}

func updatePlotterData(ctx context.Context, plotter *metrics.MetricPlotter, metricsSubscription <-chan datamodels.Metric) {
	var metrics []datamodels.Metric
	for {
		select {
		case <-ctx.Done():
			plotter.Stop()
			return

		case metric, ok := <-metricsSubscription:
			if !ok {
				slog.Error("Portfolio metrics subscription channel closed")
				return
			}
			slog.Info("Got new metric from portfolio")

			metrics = append(metrics, metric)
			plotter.UpdateMetrics(metrics)
		}
	}
}

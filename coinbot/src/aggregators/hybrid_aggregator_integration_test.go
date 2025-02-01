//go:build integration

package aggregators

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"coinbot/src/config"
	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/exchange"
	"coinbot/src/feeds"
	"coinbot/src/utils/errors"
)

func setupTest() (context.Context, context.CancelFunc, *exchange.KrakenClient, *feeds.KrakenExchangeFeed, *HybridAggregator) {
	ctx, cancel := context.WithCancel(context.Background())

	coinbotConfig, err := config.Load()
	if err != nil {
		slog.Error("Failed to load config", "error", err)
	}

	// Connect to database
	db, err := database.NewDBConnection(coinbotConfig.DatabaseConfig)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
	}

	// Connect to Exchange
	krakenClient := exchange.NewKrakenClient(coinbotConfig.KrakenConfig)
	if err := krakenClient.Connect(ctx); err != nil {
		errors.Wrap(err, "failed to connect to kraken")
	}

	realTimeFeed := feeds.NewKrakenExchangeFeed(krakenClient, datamodels.KrakenDataChannelTicker, "BTC/USD", false, nil)
	movingAverageAggregator, err := NewHybridAggregator(db, realTimeFeed, time.Minute*5, 0, MeanFunc, []string{"volume"}, []string{"mean_volume"})
	if err != nil {
		errors.Wrap(err, "failed to create moving average aggregator")
	}

	return ctx, cancel, krakenClient, realTimeFeed, movingAverageAggregator
}

func TestAggregatorE2E(t *testing.T) {

	t.Run("Create, Subscribe, Unsubscribe", func(t *testing.T) {
		ctx, cancel, krakenClient, realTimeFeed, movingAverageAggregator := setupTest()
		defer krakenClient.Close()
		defer cancel()

		// feed subscription
		feedSub, err := realTimeFeed.Subscribe(ctx, t.Name())
		if err != nil {
			errors.Wrap(err, "failed to subscribe to feed")
		}
		defer realTimeFeed.Unsubscribe(ctx, feedSub.SubscriptionId)

		// log output of aggregator
		aggSub, err := movingAverageAggregator.Subscribe(ctx, t.Name())
		if err != nil {
			errors.Wrap(err, "failed to subscribe to aggregator")
		}
		defer movingAverageAggregator.Unsubscribe(ctx, aggSub.SubscriptionId)

		aggStartErr := movingAverageAggregator.Start(ctx)
		if aggStartErr != nil {
			errors.Wrap(aggStartErr, "failed to start aggregator")
		}

		time.Sleep(time.Millisecond * 100)

		if !movingAverageAggregator.IsStarted() {
			t.Errorf("Aggregator should be started")
		}

		// feed should be started too
		if !realTimeFeed.IsStarted() {
			t.Errorf("Feed should be started")
		}

		if !krakenClient.IsConnected() {
			t.Errorf("Kraken client should be connected")
		}

		resultCount := 0
		go func() {
			for result := range aggSub.DataPointChan {
				slog.Info("Aggregator result", "result", result)
				resultCount++
			}
		}()

		time.Sleep(time.Second * 5)
		if resultCount == 0 {
			t.Errorf("No results received")
		}

		movingAverageAggregator.Stop()
		time.Sleep(time.Millisecond * 100)
		if movingAverageAggregator.IsStarted() {
			t.Errorf("Aggregator should be stopped")
		}
		// feed should not be stopped
		if !realTimeFeed.IsStarted() {
			t.Errorf("Feed should be started")
		}
	})
}

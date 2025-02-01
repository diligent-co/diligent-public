//go:build integration

package feeds

import (
	"coinbot/src/config"
	"coinbot/src/datamodels"
	"coinbot/src/exchange"
	"context"
	"testing"
	"time"
)

func TestKrakenRealtimeDataFeedIntegration(t *testing.T) {

	testDataType := datamodels.KrakenDataChannelTicker
	testPair := "ETH/USD"

	config, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	krakenConfig := config.KrakenConfig
	exchangeClient := exchange.NewKrakenClient(krakenConfig)
	feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
	feed.Start(context.Background())
	subscription, err := feed.Subscribe(context.Background(), "test_subscriber")
	if err != nil {
		t.Fatalf("Failed to subscribe to feed: %v", err)
	}

	subscriptionChannel := subscription.DataPointChan

	select {
	case dataPoint := <-subscriptionChannel:
		t.Logf("Received data point: %v", dataPoint)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for data point")
	}
	// see what comes out of the feed

}

func TestKrakenRealtimeDataFeedStartStop(t *testing.T) {

	testDataType := datamodels.KrakenDataChannelTicker
	testPair := "ETH/USD"

	config, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	exchangeClient := exchange.NewKrakenClient(config.KrakenConfig)
	feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
	feed.Start(context.Background())
	time.Sleep(1 * time.Second)
	if !feed.IsStarted() {
		t.Fatal("Feed should have started")
	}
	feed.Stop()
	time.Sleep(1 * time.Second)
	if feed.IsStarted() {
		t.Fatal("Feed should have stopped")
	}
	// subscribers should be empty
	if len(feed.subscribers) != 0 {
		t.Fatalf("Subscribers should be empty: %v", feed.subscribers)
	}
}

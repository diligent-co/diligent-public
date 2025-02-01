//go:build integration

package exchange

import (
	"context"
	"log"
	"testing"
	"time"

	"coinbot/src/config"
	"coinbot/src/datamodels"

	"github.com/stretchr/testify/assert"
)

var krakenConfig datamodels.KrakenConfig

func Init() {
	coinbotConfig, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	krakenConfig = coinbotConfig.KrakenConfig
}

func TestKrakenIntegration(t *testing.T) {
	Init()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("WebSocket Connect and Start", func(t *testing.T) {
		client := NewKrakenClient(krakenConfig)
		ctx := context.Background()

		err := client.Connect(ctx)
		assert.NoError(t, err)
		defer client.Close()

		err = client.Start(ctx)
		assert.NoError(t, err)
	})

	t.Run("Subscribe to Public Feeds", func(t *testing.T) {
		client := NewKrakenClient(krakenConfig)
		ctx := context.Background()

		err := client.Connect(ctx)
		assert.NoError(t, err)
		defer client.Close()

		err = client.Start(ctx)
		assert.NoError(t, err)

		testCases := []struct {
			name        string
			channelName datamodels.KrakenDataChannel
			pair        string
		}{
			{
				name:        "ETH/USD Ticker Feed",
				channelName: datamodels.KrakenDataChannelTicker,
				pair:        "ETH/USD",
			},
			{
				name:        "BTC/USD Ticker Feed",
				channelName: datamodels.KrakenDataChannelTicker,
				pair:        "BTC/USD",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				subscription, err := client.Subscribe(ctx, "test", tc.channelName, tc.pair)
				assert.NoError(t, err)
				assert.NotNil(t, subscription)

				// Wait for some data to arrive
				select {
				case data := <-subscription.DataPointChan:
					assert.NotNil(t, data)
				case err := <-subscription.ErrorChan:
					assert.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("Timeout waiting for subscription data")
				}

				err = client.Unsubscribe(ctx, subscription.SubscriptionId)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("REST API Endpoints", func(t *testing.T) {
		client := NewKrakenClient(krakenConfig)
		ctx := context.Background()

		t.Run("Get Server Time", func(t *testing.T) {
			serverTime, err := client.GetServerTime(ctx)
			assert.NoError(t, err)
			assert.Greater(t, serverTime, int64(0))
		})

		t.Run("Get System Status", func(t *testing.T) {
			status, err := client.GetSystemStatus(ctx)
			assert.NoError(t, err)
			assert.NotEmpty(t, status)
		})

	})

	t.Run("Private API Operations", func(t *testing.T) {
		if krakenConfig.APIKey == "" || krakenConfig.APISecret == "" {
			t.Skip("Skipping private API tests - no credentials provided")
		}

		client := NewKrakenClient(krakenConfig)
		ctx := context.Background()

		err := client.Connect(ctx)
		assert.NoError(t, err)
		defer client.Close()

		t.Run("WebSocket Token Refresh", func(t *testing.T) {
			err := client.refreshWebSocketToken()
			assert.NoError(t, err)
			assert.NotEmpty(t, client.webSocketSignature)
			assert.False(t, client.webSocketTokenExpiresAt.IsZero())
		})

		t.Run("Subscribe to Own Trades", func(t *testing.T) {
			subscription, err := client.Subscribe(ctx, "test", datamodels.KrakenDataChannelOwnTrades, "")
			assert.NoError(t, err)
			assert.NotNil(t, subscription)

			// Clean up
			err = client.Unsubscribe(ctx, subscription.SubscriptionId)
			assert.NoError(t, err)
		})
	})

	t.Run("Get Order Book", func(t *testing.T) {
		client := NewKrakenClient(krakenConfig)
		ctx := context.Background()

		// get orders after jan 1 2024
		since := int(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix())

		orderBook, lastTradeTimestamp, err := client.GetTrades(ctx, "BTC/USD", int64(since), 100)
		assert.NoError(t, err)
		assert.Greater(t, len(orderBook), 0)
		assert.Greater(t, lastTradeTimestamp, int64(0))
	})
}

//go:build unit

package feeds

import (
	"coinbot/src/datamodels"
	"context"
	"fmt"
	"testing"
)

func TestKrakenRealtimeDataFeed(t *testing.T) {
	testPair := "XBTUSD"
	testDataType := datamodels.KrakenDataChannelTicker
	ctx := context.Background()
	exchangeClient := NewMockExchangeClient()

	t.Run("Creation", func(t *testing.T) {
		feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
		if feed == nil {
			t.Fatal("Expected non-nil feed")
		}
		if feed.subscribers == nil {
			t.Fatal("Expected non-nil subscribers map")
		}
	})

	t.Run("GetName", func(t *testing.T) {
		feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
		name := feed.GetName()
		if name == "" {
			t.Fatal("Expected non-empty name")
		}
		expectedName := "KrakenExchangeFeed_ticker_XBTUSD"
		if name != expectedName {
			t.Errorf("Expected name %s, got %s", expectedName, name)
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
		ch, err := feed.Subscribe(ctx, "test_strategy")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ch == nil {
			t.Fatal("Expected non-nil channel")
		}
		if len(feed.subscribers) != 1 {
			t.Errorf("Expected 1 subscriber, got %d", len(feed.subscribers))
		}
	})

	t.Run("ChannelBuffer", func(t *testing.T) {
		exchangeClient := NewMockExchangeClient()
		feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
		sub, err := feed.Subscribe(ctx, "test_strategy")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Get the channel buffer size using reflection
		actualCh := sub.DataPointChan
		if cap(actualCh) != 100 {
			t.Errorf("Expected channel buffer size 100, got %d", cap(actualCh))
		}

		// Ensure we can receive from the channel
		select {
		case _, ok := <-actualCh:
			if !ok {
				t.Error("Channel was closed unexpectedly")
			}
		default:
			// Channel is empty as expected
		}
	})

	t.Run("MultipleSubscribers", func(t *testing.T) {
		feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
		sub1, err1 := feed.Subscribe(ctx, "strategy1")
		_, err2 := feed.Subscribe(ctx, "strategy2")

		if err1 != nil || err2 != nil {
			t.Fatal("Failed to subscribe multiple strategies")
		}

		if len(feed.subscribers) != 2 {
			t.Errorf("Expected 2 subscribers, got %d", len(feed.subscribers))
		}

		// Unsubscribe one
		err := feed.Unsubscribe(ctx, sub1.SubscriptionId)
		if err != nil {
			t.Fatalf("Failed to unsubscribe: %v", err)
		}

		if len(feed.subscribers) != 1 {
			t.Errorf("Expected 1 subscriber after unsubscribe, got %d", len(feed.subscribers))
		}
	})

	t.Run("1000 Subscribers", func(t *testing.T) {
		subscriptions := []*datamodels.DataPointSubscription{}
		feed := NewKrakenExchangeFeed(exchangeClient, testDataType, testPair, false, nil)
		for i := 0; i < 1000; i++ {
			sub, err := feed.Subscribe(ctx, fmt.Sprintf("strategy_%d", i))
			if err != nil {
				t.Fatalf("Failed to subscribe: %v", err)
			}
			subscriptions = append(subscriptions, sub)
		}

		// see if there are 1000 subscribers
		if len(feed.subscribers) != 1000 {
			t.Errorf("Expected 1000 subscribers, got %d", len(feed.subscribers))
		}

		// make sure they're not null
		for _, sub := range subscriptions {
			if sub == nil {
				t.Fatal("Expected non-nil subscription")
			}
		}

		// unsubscribe all
		for _, sub := range subscriptions {
			err := feed.Unsubscribe(ctx, sub.SubscriptionId)
			if err != nil {
				t.Fatalf("Failed to unsubscribe: %v", err)
			}
		}

		if len(feed.subscribers) != 0 {
			t.Errorf("Expected 0 subscribers after unsubscribe, got %d", len(feed.subscribers))
		}
	})
}

func TestDataPoints(t *testing.T) {
	t.Run("TestDataPointOperations", func(t *testing.T) {
		dp := NewTestDataPoint()

		// Test SetFieldValue and GetFieldValue
		err := dp.SetFieldValue("price", 100.0)
		if err != nil {
			t.Fatalf("Failed to set field value: %v", err)
		}

		value, err := dp.GetFieldValue("price")
		if err != nil {
			t.Fatalf("Failed to get field value: %v", err)
		}
		if value != 100.0 {
			t.Errorf("Expected value 100.0, got %f", value)
		}

		// Test GetFields
		fields := dp.GetFields()
		if len(fields) != 1 {
			t.Errorf("Expected 1 field, got %d", len(fields))
		}
		if fields[0] != "price" {
			t.Errorf("Expected field name 'price', got %s", fields[0])
		}

		// Test GetTimestamp
		timestamp := dp.GetTimestamp()
		if timestamp.IsZero() {
			t.Error("Expected non-zero timestamp")
		}
	})
}

package feeds

import (
	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/exchange"
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// realtime data feed

type KrakenExchangeFeed struct {
	ctx                 context.Context
	db                  database.AggFeedDatabase
	exchangeClient      exchange.ExchangeClient // Make this a KrakenClient someday?
	inboundSubscription *datamodels.DataPointSubscription
	dataType            datamodels.KrakenDataChannel
	pair                string
	subscribers         map[string]*datamodels.DataPointSubscription
	mutex               sync.Mutex
	writeToDb           bool
}

func NewKrakenExchangeFeed(
	exchangeClient exchange.ExchangeClient, // Make this a KrakenClient someday?
	dataType datamodels.KrakenDataChannel,
	pair string,
	writeToDb bool,
	db database.AggFeedDatabase,
) *KrakenExchangeFeed {

	return &KrakenExchangeFeed{
		exchangeClient: exchangeClient,
		dataType:       dataType,
		pair:           pair,
		subscribers:    make(map[string]*datamodels.DataPointSubscription),
		mutex:          sync.Mutex{},
		writeToDb:      writeToDb,
		db:             db,
	}
}

func (k *KrakenExchangeFeed) GetName() string {
	// return the struct name (without package) + dataType + pair
	thisStructName := reflect.TypeOf(k).String()
	return strings.Split(thisStructName, ".")[1] + "_" + string(k.dataType) + "_" + k.pair
}

func (k *KrakenExchangeFeed) GetOutputFieldNames() []string {
	return k.exchangeClient.GetOutputFieldNames(k.inboundSubscription.SubscriptionName)
}
func (k *KrakenExchangeFeed) Subscribe(ctx context.Context, subscriberName string) (*datamodels.DataPointSubscription, error) {
	dataChan := make(chan datamodels.DataPoint, 100) // Buffered channel to prevent blocking
	subscriptionName := fmt.Sprintf("%s_%s", subscriberName, k.GetName())
	subscriptionId := uuid.New().String()
	subscription := &datamodels.DataPointSubscription{
		SubscriptionId:   subscriptionId,
		SubscriptionName: subscriptionName,
		DataPointChan:    dataChan,
		DoneChan:         make(chan struct{}),
		ErrorChan:        make(chan error),
	}
	k.mutex.Lock()
	k.subscribers[subscriptionId] = subscription
	k.mutex.Unlock()
	return subscription, nil
}

func (k *KrakenExchangeFeed) Unsubscribe(ctx context.Context, subscriptionId string) error {
	if ch, exists := k.subscribers[subscriptionId]; exists {
		close(ch.DoneChan)
		delete(k.subscribers, subscriptionId)
	}
	return nil
}

func (k *KrakenExchangeFeed) Start(ctx context.Context) error {
	k.ctx = ctx
	k.mutex.Lock()
	if k.subscribers == nil {
		k.subscribers = make(map[string]*datamodels.DataPointSubscription)
	}
	k.mutex.Unlock()

	// subscribe to trades
	krakenSubscription, krakenSubscribeErr := k.exchangeClient.Subscribe(ctx,
		k.GetName(),
		k.dataType,
		k.pair)
	if krakenSubscribeErr != nil {
		slog.Error("Failed to subscribe to trades", "error", krakenSubscribeErr)
		return krakenSubscribeErr
	}
	k.inboundSubscription = krakenSubscription
	slog.Info("Subscribed to trades", "subscriptionName", krakenSubscription.SubscriptionName)
	slog.Info("Starting exchange client")

	exchangeStartErr := k.exchangeClient.Start(ctx) // connects and starts
	if exchangeStartErr != nil {
		slog.Error("Failed to start exchange client", "error", exchangeStartErr)
		return exchangeStartErr
	}

	slog.Info("Starting feed", "feedName", k.GetName())

	go func(inboundChan *datamodels.DataPointSubscription) {
		defer k.exchangeClient.Unsubscribe(ctx, inboundChan.SubscriptionId)

		for {
			select {
			case <-ctx.Done():
				slog.Info("Context was cancelled, stopping feed", "feedName", k.GetName())
				return
			case <-inboundChan.DoneChan:
				slog.Info("Inbound channel was closed, stopping feed", "feedName", k.GetName())
				return
			case err := <-inboundChan.ErrorChan:
				slog.Error("Error in inbound channel to feed", "feedName", k.GetName(), "error", err)
				return
			case dataPoint := <-inboundChan.DataPointChan:
				// send data point to all subscribers
				k.mutex.Lock()
				for subscriptionId, subscriberChan := range k.subscribers {
					select {
					case <-ctx.Done():
						slog.Info("Context was cancelled, stopping feed", "feedName", k.GetName())
						return
					case <-time.After(time.Second * 10):
						slog.Warn("Subscriber channel was blocked, skipping data point", "feedName", k.GetName(), "subscriptionId", subscriptionId)
						continue
					case subscriberChan.DataPointChan <- dataPoint:
						// data point sent to subscriber
					}
				}
				k.mutex.Unlock()
				if k.writeToDb {
					writeCtx, writeCancel := context.WithTimeout(k.ctx, time.Second*3)
					go func() {
						defer writeCancel()
						k.write(writeCtx, dataPoint)
					}()
				}
			}
		}
	}(krakenSubscription)

	return nil
}

func (k *KrakenExchangeFeed) IsStarted() bool {
	if k.inboundSubscription == nil {
		return false
	}

	// check if channel is closed
	select {
	case <-k.inboundSubscription.DoneChan:
		return false
	default:
		return true
	}
}

func (k *KrakenExchangeFeed) Stop() error {
	if !k.IsStarted() {
		slog.Warn("Feed is not started, cannot stop")
		return nil
	}
	// close the inbound subscription
	if k.inboundSubscription == nil {
		return nil
	}
	close(k.inboundSubscription.DoneChan)

	// close subscriber channels
	k.mutex.Lock()
	for _, subscriberChan := range k.subscribers {
		close(subscriberChan.DoneChan)
	}
	k.mutex.Unlock()

	k.inboundSubscription = nil
	k.subscribers = nil
	return nil
}

func (k *KrakenExchangeFeed) GetDataBetween(ctx context.Context, since time.Time, until time.Time) ([]datamodels.DataPoint, error) {
	sinceInt := int64(since.Unix())
	untilInt := int64(until.Unix())
	limit := 1000
	finalDataPoints := []datamodels.DataPoint{}
	for {
		dataPoints, lastTradeTime, exchangeGetDataErr := k.exchangeClient.GetTrades(ctx, k.pair, sinceInt, limit)
		if exchangeGetDataErr != nil {
			return finalDataPoints, exchangeGetDataErr
		}
		finalDataPoints = append(finalDataPoints, dataPoints...)
		if lastTradeTime < untilInt {
			break
		}
		sinceInt = lastTradeTime + 1
	}

	return finalDataPoints, nil
}

func (k *KrakenExchangeFeed) write(ctx context.Context, dataPoint datamodels.DataPoint) error {
	if !k.writeToDb {
		slog.Warn(k.GetName() + "'s write() method was called, but writeToDb is false")
		return nil
	}
	if k.db == nil {
		slog.Error(k.GetName() + "'s write() method was called, but db is nil")
		return nil
	}

	feedValue := datamodels.FeedValue{
		FeedName:  k.GetName(),
		Timestamp: dataPoint.GetTimestamp(),
		FeedValue: dataPoint,
	}

	return k.db.WriteFeedValue(ctx, feedValue)
}

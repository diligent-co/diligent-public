package aggregators

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
)

type TimedAggregator struct {
	db                         database.AggFeedDatabase
	mutex                      sync.Mutex
	feed                       feeds.DataFeed
	feedSubscription           *datamodels.DataPointSubscription
	dataPoints                 []datamodels.DataPoint
	lookbackPeriod             time.Duration
	updateCadence              time.Duration
	lastCalculatedValue        datamodels.DataPoint
	aggregatorFunc             AggregatorFunction
	aggregatorInputFields      []string
	aggregatorOutputFields     []string
	subscribers                map[string]*datamodels.DataPointSubscription
	totalInboundDataPointCount int
}

func NewTimedAggregator(
	db database.AggFeedDatabase,
	feed feeds.DataFeed,
	lookbackPeriod time.Duration,
	updateCadence time.Duration,
	aggregatorFunc AggregatorFunction,
	aggregatorInputFields []string,
	aggregatorOutputFields []string) *TimedAggregator {

	if lookbackPeriod == 0 {
		slog.Warn("Lookback period is 0 for new timed aggregator, defaulting to 1 minute")
		lookbackPeriod = 1 * time.Minute
	}

	return &TimedAggregator{
		db:                         db,
		feed:                       feed,
		lookbackPeriod:             lookbackPeriod,
		updateCadence:              updateCadence,
		dataPoints:                 make([]datamodels.DataPoint, 0),
		lastCalculatedValue:        nil,
		aggregatorFunc:             aggregatorFunc,
		aggregatorInputFields:      aggregatorInputFields,
		aggregatorOutputFields:     aggregatorOutputFields,
		subscribers:                make(map[string]*datamodels.DataPointSubscription),
		totalInboundDataPointCount: 0,
	}
}

func (a *TimedAggregator) GetName() string {
	thisStructNameWithoutPackage := strings.Split(reflect.TypeOf(a).String(), ".")[1]
	funcName := strings.Split(runtime.FuncForPC(reflect.ValueOf(a.aggregatorFunc).Pointer()).Name(), ".")[1]
	fieldsStr := strings.Join(a.aggregatorInputFields, "|") + a.lookbackPeriod.String() + ">" + strings.Join(a.aggregatorOutputFields, "|")
	var feedName string
	if a.feed != nil {
		feedName = a.feed.GetName()
	}
	return fmt.Sprintf("%s_%s_%s_%s",
		thisStructNameWithoutPackage,
		funcName,
		fieldsStr,
		feedName)
}

func (a *TimedAggregator) GetInputFieldNames() []string {
	return a.aggregatorInputFields
}

func (a *TimedAggregator) GetOutputFieldNames() []string {
	return a.aggregatorOutputFields
}

func (a *TimedAggregator) Start(ctx context.Context) error {
	// timed aggregators can run without a feed
	thisSubscriberName := a.GetName()
	processingCtx, cancelProcessingCtx := context.WithCancel(ctx)

	if a.feed == nil {
		slog.Info("Starting timed aggregator with no feed,", 
			"fields", a.aggregatorInputFields,
			"updateCadence", a.updateCadence)
	} else {
		feedName := a.feed.GetName()
		slog.Info("Starting timed aggregator",
			"feed", feedName,
			"fields", a.aggregatorInputFields,
			"updateCadence", a.updateCadence)

		// Subscribe to feed
		inboundFeedSubscription, err := a.feed.Subscribe(ctx, thisSubscriberName)
		if err != nil {
			cancelProcessingCtx()
			return fmt.Errorf("failed to subscribe to feed: %w", err)
		}

		// Set the subscription before starting goroutines
		a.feedSubscription = inboundFeedSubscription

		// Start the feed
		if !a.feed.IsStarted() {
			slog.Info("Inbound feed is not started, starting", "feed", feedName)
			if err := a.feed.Start(ctx); err != nil {
				cancelProcessingCtx()
				return fmt.Errorf("failed to start feed: %w", err)
			}
		}

		// Data collection goroutine
		go func() {
			defer a.feed.Unsubscribe(ctx, thisSubscriberName)

			for {
				select {
				case <-ctx.Done():
					slog.Info("Timed aggregator data collection routine got done signal from context", "aggregator", a.GetName())
					cancelProcessingCtx()
					return
				case _, ok := <-inboundFeedSubscription.DoneChan:
					if !ok {
						slog.Error("Timed aggregator's feed has closed its done channel", "aggregator", a.GetName())
						return
					}
					slog.Info("Timed aggregator got done signal from feed", "aggregator", a.GetName())
					a.broadcastDone()
					cancelProcessingCtx()
					return
				case err, ok := <-inboundFeedSubscription.ErrorChan:
					if !ok {
						slog.Error("Timed aggregator's feed has closed its error channel", "aggregator", a.GetName())
						return
					}
					slog.Error("Timed aggregator got error from feed", "error", err, "aggregator", a.GetName())
					a.broadcastError(err)
				case dataPoint, ok := <-inboundFeedSubscription.DataPointChan:
					if !ok {
						slog.Error("Timed aggregator's feed has closed its data point channel", "aggregator", a.GetName())
						a.broadcastDone()
						return
					}
					
					a.incrementTotalInboundDataPointCount()
					// Verify all required fields are present
					fields := dataPoint.GetFields()
					inputFields := a.getInputFields()
					for _, field := range inputFields {
						if _, exists := fields[field]; !exists {
							slog.Error("Required field missing in data point",
								"field", field,
								"availableFields", fields)
							continue
						}
					}

					a.addDataPointAndRemoveOld(dataPoint)
				}
			}
		}()
	}

	// Processing goroutine
	go func(ctx context.Context, cancelProcessingCtx context.CancelFunc) {
		ticker := time.NewTicker(a.updateCadence)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("Timed aggregator processing routine got done signal from context", "aggregator", a.GetName())
				return
			case <-ticker.C:
				dataPoints := a.GetDataPoints()
				
				inputFields := a.getInputFields()
				outputFields := a.getOutputFields()

				// Calculate new value
				result, err := a.aggregatorFunc(dataPoints,
					inputFields,
					outputFields)
				if err != nil {
					slog.Error("Error calculating aggregate value",
						"error", err,
						"numDataPoints", len(dataPoints))
					a.broadcastError(err)
					continue
				}
				
				if result == nil {
					lastValue := a.getLastCalculatedValue()
					subscribers := a.GetSubscribers()
					if lastValue == nil {
						slog.Warn("Last calculated value is nil, skipping broadcast", "aggregator", a.GetName())
					} else if len(subscribers) > 0 {
						// Broadcast last known value if we have no new data
						a.broadcastDataPoint(lastValue)
					}
					continue
				}

				a.setLastCalculatedValue(result)

				// Broadcast to subscribers
				a.broadcastDataPoint(result)
			}
		}
	}(processingCtx, cancelProcessingCtx)

	return nil
}

func (a *TimedAggregator) IsStarted() bool {
	// criteria: inboundFeedSubscription is not nil and done signal is not sent
	if a.feedSubscription == nil {
		return false
	}
	select {
	case <-a.feedSubscription.DoneChan:
		return false
	default:
		return true
	}
}

func (a *TimedAggregator) Stop() error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Close all subscriber channels
	for id, subscriber := range a.subscribers {
		// if chans are open, close them
		if subscriber.DataPointChan != nil {
			close(subscriber.DataPointChan)
		}
		if subscriber.DoneChan != nil {
			close(subscriber.DoneChan)
		}
		if subscriber.ErrorChan != nil {
			close(subscriber.ErrorChan)
		}
		delete(a.subscribers, id)
	}

	return nil
}

func (a *TimedAggregator) GetDataPoints() []datamodels.DataPoint {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Return a copy of the dataPoints slice to prevent external modification
	result := make([]datamodels.DataPoint, len(a.dataPoints))
	copy(result, a.dataPoints)
	return result
}

func (a *TimedAggregator) getLastCalculatedValue() datamodels.DataPoint {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.lastCalculatedValue
}

func (a *TimedAggregator) setLastCalculatedValue(dp datamodels.DataPoint) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.lastCalculatedValue = dp
}

func (a *TimedAggregator) GetTotalInboundDataPointCount() int {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.totalInboundDataPointCount
}

func (a *TimedAggregator) Subscribe(ctx context.Context,
	subscriberName string) (*datamodels.DataPointSubscription, error) {
	dataChan := make(chan datamodels.DataPoint, 100)
	subscriptionName := fmt.Sprintf("%s_%s", subscriberName, a.GetName())
	subscriptionId := uuid.New().String()
	subscription := &datamodels.DataPointSubscription{
		SubscriptionId:   subscriptionId,
		SubscriptionName: subscriptionName,
		DataPointChan:    dataChan,
		DoneChan:         make(chan struct{}),
		ErrorChan:        make(chan error),
	}

	a.addSubscriber(subscription)
	return subscription, nil
}

func (a *TimedAggregator) Unsubscribe(ctx context.Context, subscriptionId string) error {
	a.removeSubscriber(subscriptionId)
	return nil
}

func (a *TimedAggregator) GetSubscribers() map[string]*datamodels.DataPointSubscription {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.subscribers
}

func (a *TimedAggregator) GetLastCalculatedValue() datamodels.DataPoint {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.lastCalculatedValue == nil {
		slog.Error("Last calculated value is nil for aggregator", "aggregator", a.GetName())
	}
	return a.lastCalculatedValue
}

func (a *TimedAggregator) incrementTotalInboundDataPointCount() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.totalInboundDataPointCount++
}

func (a *TimedAggregator) addDataPointAndRemoveOld(newDataPoint datamodels.DataPoint) {
	a.mutex.Lock()
	// Remove old data points
	cutoffTime := newDataPoint.GetTimestamp().Add(-a.lookbackPeriod)
	cutoffIndex := 0
	for i := len(a.dataPoints) - 1; i >= 0; i-- {
		if a.dataPoints[i].GetTimestamp().Before(cutoffTime) {
			cutoffIndex = i + 1
			break
		}
	}
	a.dataPoints = a.dataPoints[cutoffIndex:]
	a.dataPoints = append(a.dataPoints, newDataPoint)
	a.mutex.Unlock()
}

func (a *TimedAggregator) getInputFields() []string {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	copiedInputFields := make([]string, len(a.aggregatorInputFields))
	copy(copiedInputFields, a.aggregatorInputFields)
	return copiedInputFields
}

func (a *TimedAggregator) getOutputFields() []string {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	copiedOutputFields := make([]string, len(a.aggregatorOutputFields))
	copy(copiedOutputFields, a.aggregatorOutputFields)
	return copiedOutputFields
}

func (a *TimedAggregator) addSubscriber(subscription *datamodels.DataPointSubscription) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.subscribers[subscription.SubscriptionId] = subscription
}

func (a *TimedAggregator) removeSubscriber(subscriptionId string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.subscribers, subscriptionId)
}

func (a *TimedAggregator) broadcastDataPoint(dp datamodels.DataPoint) {
	subscribers := a.GetSubscribers()
	for _, subscriber := range subscribers {
		if subscriber.DataPointChan == nil {
			slog.Error("Timed aggregator's subscriber has nil data point channel", "aggregator", a.GetName(), "subscriber", subscriber.SubscriptionName)
			continue
		}
		select {
		case subscriber.DataPointChan <- dp:
			// Could add outbound count tracking if needed
		case <-time.After(time.Second * 1):
			slog.Warn("aggregator subscription channel full, skipping update",
				"channel_current_size", len(subscriber.DataPointChan),
				"channel_capacity", cap(subscriber.DataPointChan),
				"aggregatorName", a.GetName(),
				"subscriberName", subscriber.SubscriptionName)
		}
	}
}

func (a *TimedAggregator) broadcastDone() {
	subscribers := a.GetSubscribers()

	// Propagate done signal to all subscribers when feed is done
	for _, subscriber := range subscribers {
		slog.Info("Aggregator broadcasting done signal to subscriber",
			"aggregator", a.GetName(),
			"subscriber", subscriber.SubscriptionName)
		subscriber.DoneChan <- struct{}{}
	}
}

func (a *TimedAggregator) broadcastError(err error) {
	subscribers := a.GetSubscribers()
	for _, subscriber := range subscribers {
		subscriber.ErrorChan <- err
	}
}

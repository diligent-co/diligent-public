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
	"coinbot/src/utils/errors"
	"coinbot/src/utils/general"
)

// Every StrategyEngine has an Aggregator
// The Aggregator is responsible for aggregating the data from the DataFeeds
// and passing it to the Strategy, which will then make trading decisions
// The Aggregator is also responsible for persisting the data to the database

// generic aggregator that can pair a function and a field in a feed
type HybridAggregator struct {
	db                          database.AggFeedDatabase
	mutex                       sync.Mutex
	feed                        feeds.DataFeed
	feedSubscription            *datamodels.DataPointSubscription
	dataPoints                  []datamodels.DataPoint
	lookbackPeriod              time.Duration
	minUpdateCadence            time.Duration
	lastCalculatedValue         datamodels.DataPoint
	aggregatorFunc              AggregatorFunction
	aggregatorInputFields       []string
	aggregatorOutputFields      []string
	subscribers                 map[string]*datamodels.DataPointSubscription
	totalInboundDataPointCount  int
	totalOutboundDataPointCount int
}

func NewHybridAggregator(
	db database.AggFeedDatabase,
	feed feeds.DataFeed,
	lookbackPeriod time.Duration,
	minUpdateCadence time.Duration,
	aggregatorFunc AggregatorFunction,
	aggregatorInputFields []string,
	aggregatorOutputFields []string) (*HybridAggregator, error) {

	if minUpdateCadence <= 0 || minUpdateCadence > 10*time.Minute {
		slog.Warn("Invalid minUpdateCadence, setting to 1 minute", "minUpdateCadence", minUpdateCadence)
		minUpdateCadence = 1 * time.Minute
	}
	feedOutputFields := feed.GetOutputFieldNames()
	for _, field := range aggregatorInputFields {
		if !general.ItemInSlice(feedOutputFields, field) {
			slog.Error("Field not found in feed output fields", "field", field, "feed", feed.GetName())
			return nil, fmt.Errorf("field not found in feed output fields: %s", field)
		}
	}

	return &HybridAggregator{
		db:                          db,
		feed:                        feed,
		lookbackPeriod:              lookbackPeriod,
		minUpdateCadence:            minUpdateCadence,
		dataPoints:                  make([]datamodels.DataPoint, 0),
		lastCalculatedValue:         nil,
		aggregatorFunc:              aggregatorFunc,
		aggregatorInputFields:       aggregatorInputFields,
		aggregatorOutputFields:      aggregatorOutputFields,
		subscribers:                 make(map[string]*datamodels.DataPointSubscription),
		totalInboundDataPointCount:  0,
		totalOutboundDataPointCount: 0,
	}, nil
}

func (a *HybridAggregator) GetName() string {
	thisStructNameWithoutPackage := strings.Split(reflect.TypeOf(a).String(), ".")[1]
	funcName := strings.Split(runtime.FuncForPC(reflect.ValueOf(a.aggregatorFunc).Pointer()).Name(), ".")[1]
	fieldsStr := strings.Join(a.aggregatorInputFields, "|") + a.lookbackPeriod.String() + ">" + strings.Join(a.aggregatorOutputFields, "|")
	return fmt.Sprintf("%s_%s_%s_%s",
		thisStructNameWithoutPackage,
		funcName,
		fieldsStr,
		a.feed.GetName())
}

func (a *HybridAggregator) GetInputFieldNames() []string {
	return a.aggregatorInputFields
}

func (a *HybridAggregator) GetOutputFieldNames() []string {
	return a.aggregatorOutputFields
}

func (a *HybridAggregator) GetDataPoints() []datamodels.DataPoint {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	copiedDataPoints := make([]datamodels.DataPoint, len(a.dataPoints))
	for i, dp := range a.dataPoints {
		copiedDataPoints[i] = dp.Copy()
	}
	return copiedDataPoints
}

func (a *HybridAggregator) getInputFields() []string {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	copiedInputFields := make([]string, len(a.aggregatorInputFields))
	copy(copiedInputFields, a.aggregatorInputFields)
	return copiedInputFields
}

func (a *HybridAggregator) getOutputFields() []string {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	copiedOutputFields := make([]string, len(a.aggregatorOutputFields))
	copy(copiedOutputFields, a.aggregatorOutputFields)
	return copiedOutputFields
}

func (a *HybridAggregator) incrementTotalInboundDataPointCount() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.totalInboundDataPointCount++
}

func (a *HybridAggregator) incrementTotalOutboundDataPointCount() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.totalOutboundDataPointCount++
}

func (a *HybridAggregator) setDataPoints(datapoints []datamodels.DataPoint) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.dataPoints = datapoints
}

func (a *HybridAggregator) addDataPoint(dp datamodels.DataPoint) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.dataPoints = append(a.dataPoints, dp)
}

func (a *HybridAggregator) GetTotalInboundDataPointCount() int {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.totalInboundDataPointCount
}

func (a *HybridAggregator) GetLastCalculatedValue() datamodels.DataPoint {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.lastCalculatedValue.Copy()
}

func (a *HybridAggregator) setLastCalculatedValue(dp datamodels.DataPoint) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.lastCalculatedValue = dp.Copy()
}

func (a *HybridAggregator) addSubscriber(subscription *datamodels.DataPointSubscription) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.subscribers[subscription.SubscriptionId] = subscription
}

func (a *HybridAggregator) removeSubscriber(subscriptionId string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.subscribers, subscriptionId)
}

func (a *HybridAggregator) GetSubscribers() map[string]*datamodels.DataPointSubscription {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	subscribers := make(map[string]*datamodels.DataPointSubscription)
	for id, subscription := range a.subscribers {
		subscribers[id] = subscription
	}
	return subscribers
}

func (a *HybridAggregator) Start(ctx context.Context) error {
	thisSubscriberName := a.GetName()
	slog.Info("Starting aggregator", "aggregator", a.GetName())
	slog.Info("Subscribing to feed", "feed", a.feed.GetName(), "subscriberName", thisSubscriberName)
	// Note from Logan: surely if the feed is nil we'd error out on a.feed.Subscribe(), right? I moved the nil check to the top to be safe.
	if a.feed == nil {
		slog.Error("Feed is nil, cannot start aggregator", "aggregator", a.GetName())
		return errors.New("feed is nil")
	}
	inboundFeedSubscription, err := a.feed.Subscribe(ctx, thisSubscriberName)
	if err != nil {
		return err
	}
	if !a.feed.IsStarted() {
		slog.Info("Inbound feed is not started, starting", "feed", a.feed.GetName())
		feedStartErr := a.feed.Start(ctx)
		if feedStartErr != nil {
			slog.Error("Failed to start feed", "error", feedStartErr)
			return feedStartErr
		}
		slog.Info("Aggregator successfully started feed", "feed", a.feed.GetName())
	}

	slog.Info("Aggregator successfully subscribed to feed", "aggregator", a.GetName(), "feed", a.feed.GetName())

	ticker := time.NewTicker(a.minUpdateCadence)
	defer ticker.Stop()

	go func(sub *datamodels.DataPointSubscription) {
		defer a.feed.Unsubscribe(ctx, thisSubscriberName)
		defer func() {
			a.filterAggregateBroadcast()
			a.broadcastDone()
		}()

		// Helper function to calculate and broadcast value

		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-sub.DoneChan:
				if !ok {
					slog.Error("Hybrid aggregator's feed has closed its done channel", "aggregator", a.GetName())
					return
				}
				slog.Info("Hybrid aggregator's feed has sent done signal", "aggregator", a.GetName())
				return
			case err, ok := <-sub.ErrorChan:
				if !ok {
					slog.Error("Hybrid aggregator's feed has closed its error channel", "aggregator", a.GetName())
					return
				}
				slog.Error("Hybrid aggregator's feed has sent error signal", "error", err, "aggregator", a.GetName())
				a.broadcastError(err)
			case dataPoint, ok := <-sub.DataPointChan:
				if !ok {
					slog.Error("Hybrid aggregator's feed has closed its data point channel", "aggregator", a.GetName())
					return
				}
				a.incrementTotalInboundDataPointCount()
				aggInputFields := a.getInputFields()
				// Verify fields exist in dataPoint
				validPoint := true
				for _, field := range aggInputFields {
					if _, exists := dataPoint.GetFields()[field]; !exists {
						slog.Error("Field not found in data point", "field", field, "dataPoint", dataPoint)
						validPoint = false
						a.broadcastError(fmt.Errorf("field not found in data point: %s", field))
					}
				}
				if !validPoint {
					continue
				}

				a.addDataPoint(dataPoint)
				a.filterAggregateBroadcast()

				// Reset timer since we just processed new data
				ticker.Reset(a.minUpdateCadence)

			case <-ticker.C:
				a.filterAggregateBroadcast()
			}
		}
	}(inboundFeedSubscription)

	a.feedSubscription = inboundFeedSubscription
	return nil
}

func (a *HybridAggregator) IsStarted() bool {
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

func (a *HybridAggregator) Stop() error {
	if !a.IsStarted() {
		slog.Warn("Aggregator is not started, cannot stop")
		return nil
	}
	// stop reading from the feed
	if a.feedSubscription != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		slog.Info("Stopping moving average aggregator, unsubscribing from feed", "feed", a.feed.GetName())
		unsubErr := a.feed.Unsubscribe(ctx, a.feedSubscription.SubscriptionId)
		if unsubErr != nil {
			slog.Error("Error unsubscribing from feed", "error", unsubErr)
		}
		slog.Info("Successfully unsubscribed from feed", "feed", a.feed.GetName())
		slog.Info("Stopped moving average aggregator", "feed", a.feed.GetName())
	} else {
		slog.Error("Feed subscription is nil, cannot unsubscribe")
	}

	subscribers := a.GetSubscribers()
	if len(subscribers) == 0 {
		slog.Warn("No subscribers, skipping close")
	} else {
		// close outbound channels
		a.broadcastDone()
	}
	return nil
}

func (a *HybridAggregator) Subscribe(ctx context.Context,
	subscriberName string) (*datamodels.DataPointSubscription, error) {
	dataChan := make(chan datamodels.DataPoint, 500)
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

func (a *HybridAggregator) Unsubscribe(ctx context.Context, subscriptionId string) error {
	a.removeSubscriber(subscriptionId)
	return nil
}

func (a *HybridAggregator) broadcastDataPoint(dp datamodels.DataPoint) {
	subscribers := a.GetSubscribers()
	for _, subscriber := range subscribers {
		select {
		case subscriber.DataPointChan <- dp:
			a.incrementTotalOutboundDataPointCount()
		case <-time.After(time.Second * 1):
			slog.Warn("aggregator subscription channel full, skipping update",
				"channel_current_size", len(subscriber.DataPointChan),
				"channel_capacity", cap(subscriber.DataPointChan),
				"aggregatorName", a.GetName(),
				"subscriberName", subscriber.SubscriptionName)
		}
	}
}

func (a *HybridAggregator) broadcastDone() {
	subscribers := a.GetSubscribers()

	// Propagate done signal to all subscribers when feed is done
	for _, subscriber := range subscribers {
		slog.Info("Hybrid aggregator broadcasting done signal to subscriber",
			"subscriber", subscriber.SubscriptionName)
		select {
		case subscriber.DoneChan <- struct{}{}:
		case <-time.After(time.Second * 1):
			slog.Warn("DoneChan already closed or full",
				"aggregator", a.GetName(),
				"subscriber", subscriber.SubscriptionName)
		}
	}
}

func (a *HybridAggregator) broadcastError(err error) {
	subscribers := a.GetSubscribers()
	for _, subscriber := range subscribers {
		select {
		case subscriber.ErrorChan <- err:
		case <-time.After(time.Second * 1):
			slog.Warn("ErrorChan closed or full",
				"aggregator", a.GetName(),
				"subscriber", subscriber.SubscriptionName)
		}
	}
}

// func (a *HybridAggregator) GetValueAsOfTime(ctx context.Context, asOfTime time.Time) ([]datamodels.DataPoint, error) {
// 	// go from asOfTime back by a.period
// 	startTime := asOfTime.Add(-a.lookbackPeriod)
// 	dataPoints, err := a.feed.GetDataBetween(ctx, startTime, asOfTime)
// 	if err != nil {
// 		return nil, err
// 	}

// 	result, err := a.aggregatorFunc(dataPoints, a.aggregatorInputFields, a.aggregatorOutputField)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return []datamodels.DataPoint{result}, nil
// }

func (a *HybridAggregator) filterAggregateBroadcast() {
	datapoints := a.GetDataPoints()
	// called when there is a new datapoint or when the ticker goes off

	if len(datapoints) == 0 {
		return
	}

	// Get cutoff time based on current (stream) time and lookback period
	lookbackPeriod := a.lookbackPeriod
	latestTime := datapoints[len(datapoints)-1].GetTimestamp()
	cutoffTime := latestTime.Add(-lookbackPeriod)

	// Find cutoff index by iterating backwards until we find first stale point
	cutoffIndex := 0
	lastIndex := len(datapoints) - 1
	for i := lastIndex; i >= 0; i-- {
		dpIisBeforeCutoffTime := datapoints[i].GetTimestamp().Before(cutoffTime)
		if dpIisBeforeCutoffTime {
			cutoffIndex = i
			break
		}
	}

	// Keep only points after cutoff index
	datapoints = datapoints[cutoffIndex:]
	a.setDataPoints(datapoints)

	inputFields := a.getInputFields()
	outputFields := a.getOutputFields()
	// Calculate new value
	result, err := a.aggregatorFunc(datapoints, inputFields, outputFields)
	if err != nil {
		slog.Error("Error calculating aggregate value", "error", err)
		return
	}

	// Store and broadcast
	a.setLastCalculatedValue(result)
	a.broadcastDataPoint(result)
}

// func (a *GenericAggregator) startCheckpointing(ctx context.Context) error {
// 	if a.checkpointInterval == 0 {
// 		slog.Info("Checkpoint interval for aggregator is 0, so not checkpointing", "aggregator", a.GetName())
// 		return nil
// 	}
// 	go func() {
// 		ticker := time.NewTicker(a.checkpointInterval)
// 		defer ticker.Stop()
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case <-ticker.C:
// 				asOfTime := time.Now()
// 				latestValue, err := a.GetValueAsOfTime(ctx, asOfTime)
// 				if err != nil {
// 					slog.Error("Error getting latest value", "error", err)
// 					continue
// 				}
// 				a.db.WriteAggregatorValue(ctx, datamodels.AggregatorValue{
// 					AggregatorName: a.GetName(),
// 					AsOfTime:       asOfTime, //
// 					Value:          latestValue[0],
// 				})
// 			}
// 		}
// 	}()
// 	return nil
// }

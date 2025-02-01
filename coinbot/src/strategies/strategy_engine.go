package strategies

/*
A strategy consumes data from feeds or aggregators, and emits signals.
Both feeds and aggegators use DataPointSubscriptions, so that's probably the right abstraction

A strategy can subscribe to multiple feeds or aggregators.
*/

import (
	"coinbot/src/aggregators"
	"coinbot/src/datamodels"
	"coinbot/src/metrics"
	"coinbot/src/utils/errors"
	"coinbot/src/utils/general"
	"context"

	// "encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const chanWaitTime = time.Second

type StrategySubscription struct {
	SubscriptionName string
	SubscriptionId   string
	ReadyChan        chan struct{}
	SignalChan       chan datamodels.Signal
	ErrorChan        chan error
	DoneChan         chan struct{}
	FeedbackChan     chan datamodels.RewardRecord
}

type strategyEngine struct {
	id                         string
	name                       string
	ctx                        context.Context
	aggregators                map[string]aggregators.Aggregator
	aggregatorBuffer           map[string][]datamodels.DataPoint
	aggregatorBufferSize       int
	aggHasData                 map[string]bool
	aggregatorsHaveDataChannel chan string
	newDataReceivedChannel     chan string
	aggregatorsDoneChannel     chan string
	mutex                      sync.RWMutex
	signalBuffer               *general.TimedBuffer[*datamodels.Signal]
	signalFuncSupplier         SignalFunctionSupplier
	subscribers                map[string]StrategySubscription
	signalRewardChannel        chan datamodels.RewardRecord
	rewardBuffer               *general.TimedBuffer[*datamodels.RewardRecord]
	dataStartTime              time.Time
	stepCadence                time.Duration
	realTime                   bool
	dataStreamTime             time.Time
	lastSignalTime             time.Time
	metricsWriter              metrics.MetricsWriter
}

func NewStratEngine() *strategyEngine {
	id := general.GenerateUUID5StringFromByteArray([]byte(time.Now().String()))
	bufferSize := 1000
	return &strategyEngine{
		id:                         id,
		aggregators:                make(map[string]aggregators.Aggregator),
		aggregatorBuffer:           make(map[string][]datamodels.DataPoint),
		aggregatorBufferSize:       bufferSize,
		aggHasData:                 make(map[string]bool),
		aggregatorsHaveDataChannel: make(chan string, 50),
		newDataReceivedChannel:     make(chan string, bufferSize),
		aggregatorsDoneChannel:     make(chan string, 50),
		mutex:                      sync.RWMutex{},
		signalBuffer:               general.NewTimedBuffer[*datamodels.Signal](bufferSize),
		subscribers:                make(map[string]StrategySubscription),
		signalRewardChannel:        make(chan datamodels.RewardRecord, 100),
		rewardBuffer:               general.NewTimedBuffer[*datamodels.RewardRecord](bufferSize),
		stepCadence:                0,
		realTime:                   true,
		metricsWriter:              nil,
	}
}

func (se *strategyEngine) SetName(name string) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.name = name
}

func (se *strategyEngine) WithSetName(name string) *strategyEngine {
	se.SetName(name)
	return se
}

func (se *strategyEngine) GetDataStartTime() time.Time {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.dataStartTime
}

func (se *strategyEngine) GetName() string {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	// get struct name without package path
	structName := strings.Split(reflect.TypeOf(se).String(), ".")[1]
	if se.name == "" {
		aggregatorNames := []string{}
		for aggName := range se.aggregators {
			aggregatorNames = append(aggregatorNames, aggName)
		}
		return structName + "_" + strings.Join(aggregatorNames, "|")
	}
	return se.name
}

func (se *strategyEngine) WithAggregator(name string, aggregator aggregators.Aggregator) *strategyEngine {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.aggregators[name] = aggregator
	se.aggregatorBuffer[name] = []datamodels.DataPoint{}
	slog.Debug("Added aggregator to strategy", "name", name)
	return se
}

func (se *strategyEngine) WithAggregators(aggregators map[string]aggregators.Aggregator) *strategyEngine {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.aggregators = aggregators
	return se
}

func (se *strategyEngine) WithSignalFunctionSupplier(signalFuncSupplier SignalFunctionSupplier) *strategyEngine {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.signalFuncSupplier = signalFuncSupplier
	return se
}

func (se *strategyEngine) WithBufferSize(bufferSize int) *strategyEngine {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.aggregatorBufferSize = bufferSize
	return se
}

func (se *strategyEngine) WithStepCadence(stepCadence time.Duration) *strategyEngine {
	if stepCadence < 0 {
		slog.Error("Step cadence cannot be negative")
		return se
	}
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.stepCadence = stepCadence
	return se
}

func (se *strategyEngine) WithRealTime(realTime bool) *strategyEngine {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.realTime = realTime
	return se
}

func (se *strategyEngine) WithMetricsWriter(metricsWriter metrics.MetricsWriter) *strategyEngine {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.metricsWriter = metricsWriter
	return se
}

func (se *strategyEngine) setAggHasData(name string, hasData bool) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.aggHasData[name] = hasData
}

func (se *strategyEngine) getAggHasData(name string) bool {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	hasData, ok := se.aggHasData[name]
	if !ok {
		slog.Error("Aggregator has data not found", "aggregatorName", name)
		return false
	}
	return hasData
}

func (se *strategyEngine) getStepCadence() time.Duration {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.stepCadence
}

func (se *strategyEngine) getRealTime() bool {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.realTime
}

func (se *strategyEngine) getBufferSize() int {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.aggregatorBufferSize
}

func (se *strategyEngine) getAggregators() map[string]aggregators.Aggregator {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.aggregators
}

func (se *strategyEngine) getAggregator(name string) aggregators.Aggregator {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.aggregators[name]
}

func (se *strategyEngine) setAggregator(name string, aggregator aggregators.Aggregator) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.aggregators[name] = aggregator
}

func (se *strategyEngine) removeAggregator(name string) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	delete(se.aggregators, name)
	delete(se.aggregatorBuffer, name)
	delete(se.aggHasData, name)
}

func (se *strategyEngine) unsubscribeFromAggregator(name string) error {
	aggregator := se.getAggregator(name)
	if aggregator == nil {
		slog.Error("Aggregator not found", "aggregatorName", name)
		return errors.New("aggregator not found")
	}
	unsubErr := aggregator.Unsubscribe(se.ctx, se.GetName())
	if unsubErr != nil {
		se.broadcastError(unsubErr)
		return errors.Wrap(unsubErr, fmt.Sprintf("error unsubscribing from aggregator %s", name))
	}
	se.removeAggregator(name)
	return nil
}

func (se *strategyEngine) getAllAggregatorData() map[string][]datamodels.DataPoint {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	buffersCopy := make(map[string][]datamodels.DataPoint)
	for aggName, buffer := range se.aggregatorBuffer {
		dps := make([]datamodels.DataPoint, len(buffer))
		for i, dp := range buffer {
			dps[i] = dp.Copy()
		}
		buffersCopy[aggName] = dps
	}
	return buffersCopy
}

func (se *strategyEngine) getAggregatorBuffer(name string) []datamodels.DataPoint {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.aggregatorBuffer[name]
}

func (se *strategyEngine) setAggregatorBuffer(name string, buffer []datamodels.DataPoint) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.aggregatorBuffer[name] = buffer
}

func (se *strategyEngine) setDataStreamTime(dataStreamTime time.Time) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.dataStreamTime = dataStreamTime
}

func (se *strategyEngine) getDataStreamTime() time.Time {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.dataStreamTime
}

func (se *strategyEngine) addSignalToBuffer(signal *datamodels.Signal) {
	// implements mutex internally
	se.signalBuffer.AddElement(signal)
}

func (se *strategyEngine) setLastSignalTime(lastSignalTime time.Time) {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	se.lastSignalTime = lastSignalTime
}

func (se *strategyEngine) getLastSignalTime() time.Time {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.lastSignalTime
}

func (se *strategyEngine) Build() (*strategyEngine, error) {
	if se.getBufferSize() <= 0 {
		slog.Error("Buffer size must be greater than 0")
		return nil, errors.New("buffer size must be greater than 0")
	}
	if se.signalFuncSupplier == nil {
		slog.Error("No signal function provided to strategy")
		return nil, errors.New("no signal function provided to strategy")
	}

	// check if feeds and aggregators have been added
	aggregators := se.getAggregators()
	if len(aggregators) == 0 {
		slog.Error("No aggregators added to strategy")
		return nil, errors.New("no aggregators added to strategy")
	}

	neededFields := se.signalFuncSupplier.GetInputFields()
	// make sure all needed fields are present
	for _, field := range neededFields {
		if _, ok := aggregators[field]; !ok {
			allAggregatorNames := []string{}
			for aggName := range aggregators {
				allAggregatorNames = append(allAggregatorNames, aggName)
			}
			return nil, errors.New(fmt.Sprintf("missing field in aggregator data: %s, aggregators: %s", field, strings.Join(allAggregatorNames, ", ")))
		}
	}

	// for each aggregator, initialize aggHasData to false
	for aggName, _ := range aggregators {
		se.setAggHasData(aggName, false)
	}

	if se.stepCadence == 0 {
		slog.Info("Step cadence is 0, which equates to per-event signal checking")
	}
	return se, nil
}

func (se *strategyEngine) Start(ctx context.Context) error {
	if se.IsStarted() {
		slog.Warn("Strategy already started, skipping start", "name", se.GetName())
		return nil
	}
	if ctx == nil {
		slog.Error("Context is nil, cannot start strategy")
		return errors.New("context is nil")
	}
	se.ctx = ctx
	slog.Info("Starting strategy", "name", se.GetName())

	aggregators := se.getAggregators()

	// Then subscribe to all aggregators
	for aggName, aggregator := range aggregators {
		aggSub, err := aggregator.Subscribe(ctx, se.GetName())
		if err != nil {
			slog.Error("Error subscribing to aggregator", "aggregatorName", aggName, "error", err)
			se.broadcastError(err)
			return err
		}
		go se.handleAggregatorUpdates(aggName, aggSub)
	}

	for aggName, aggregator := range aggregators {
		se.setAggHasData(aggName, false)
		if !aggregator.IsStarted() {
			if err := aggregator.Start(ctx); err != nil {
				slog.Error("Error starting aggregator", "aggregatorName", aggName, "error", err)
				se.broadcastError(err)
				return err
			}
		}
	}

	go se.processSignals()
	go se.handleAggregatorDoneSignals()
	go se.processSignalRewardChannel()

	return nil
}

func (se *strategyEngine) handleAggregatorUpdates(aggName string, aggSub *datamodels.DataPointSubscription) {
	for {
		select {
		case <-se.ctx.Done():
			slog.Info("Strategy context done, stopping aggregator update handler", "aggregatorName", aggName)
			return
		case <-aggSub.DoneChan:
			se.aggregatorsDoneChannel <- aggName
			return
		case err, ok := <-aggSub.ErrorChan:
			if !ok {
				slog.Warn("Error channel closed, stopping aggregator update handler", "aggregatorName", aggName)
				se.broadcastError(errors.New(fmt.Sprintf("error channel %s closed", aggName)))
				se.aggregatorsDoneChannel <- aggName
				return
			}
			slog.Error("Error received from aggregator", "aggregatorName", aggName, "error", err)
			se.broadcastError(err)
		case data, ok := <-aggSub.DataPointChan:
			if !ok {
				slog.Warn("Data point channel closed, stopping aggregator update handler", "aggregatorName", aggName)
				se.broadcastError(errors.New(fmt.Sprintf("data point channel %s closed", aggName)))
				se.aggregatorsDoneChannel <- aggName
				return
			}
			// print utilized capacity of the channel
			// breakpoint if at 90% capacity
			if general.ChannelAtLoadLevel[datamodels.DataPoint](aggSub.DataPointChan, 0.8) {
				slog.Warn("Strategy aggregator incoming data point channel load warning", "aggregator", aggName)
			}
			se.handleNewData(aggName, data)
		}
	}
}

func (se *strategyEngine) handleAggregatorDoneSignals() {
	slog.Info("Strategy listening for aggregator done signals")
	aggregatorsRemaining := len(se.getAggregators())

	for aggregatorsRemaining > 0 {
		aggName := <-se.aggregatorsDoneChannel
		aggregatorsRemaining--
		slog.Info(fmt.Sprintf("Strategy got done signal from aggregator, %d remaining", aggregatorsRemaining), "aggregatorName", aggName)
	}

	slog.Info("All aggregators done, checking and emitting last signal")
	se.checkAndEmitSignal()
	slog.Info("Last signal emitted, broadcasting done")
	se.broadcastDone()
}

func (se *strategyEngine) handleNewData(aggregatorName string, data datamodels.DataPoint) {

	buffer := se.getAggregatorBuffer(aggregatorName)
	if buffer == nil {
		slog.Error("Aggregator buffer not found", "aggregatorName", aggregatorName)
		se.broadcastError(errors.New("aggregator buffer not found"))
		return
	}
	buffer = append(buffer, data)
	if len(buffer) > se.getBufferSize() {
		buffer = buffer[1:] // Remove oldest element
	}
	se.setAggregatorBuffer(aggregatorName, buffer)

	// check if this aggregator has data
	aggHasData := se.getAggHasData(aggregatorName)
	if !aggHasData {
		se.setAggHasData(aggregatorName, true)
		if se.aggregatorsHaveDataChannel != nil {
			se.aggregatorsHaveDataChannel <- aggregatorName
		} else {
			slog.Warn("Aggregators have data channel is nil, cannot send data", "aggregatorName", aggregatorName)
		}
	}

	if data.GetTimestamp().After(se.getDataStreamTime()) {
		se.setDataStreamTime(data.GetTimestamp())
	}

	select {
	case se.newDataReceivedChannel <- aggregatorName:
		// check proportion of channel capacity
		if general.ChannelAtLoadLevel[string](se.newDataReceivedChannel, 0.8) {
			slog.Warn("Strategy engine newDataReceivedChannel load warning", "aggregator", aggregatorName)
		}
	case <-time.After(chanWaitTime):
		slog.Warn(fmt.Sprintf("After waiting, strategy engine newDataReceivedChannel is still full, dropping data from aggregator %s", aggregatorName))
	}
}

func (se *strategyEngine) waitForAggregatorsToHaveData() {
	slog.Info(fmt.Sprintf("Strategy waiting for aggregators to have data %s", se.GetName()))
	aggregatorsRemaining := len(se.getAggregators())

	for aggregatorsRemaining > 0 {
		select {
		case <-se.ctx.Done():
			return
		case aggregatorName := <-se.aggregatorsHaveDataChannel:
			slog.Info("Aggregator reported data", "aggregator", aggregatorName, "remaining", aggregatorsRemaining)
			aggregatorsRemaining--
		}
	}
	slog.Info("All aggregators have data for strategy")
	se.broadcastReady()
}

func (se *strategyEngine) processSignals() {
	se.waitForAggregatorsToHaveData()

	stepCadence := se.getStepCadence()
	isRealTime := se.getRealTime()
	var timer *time.Ticker
	if isRealTime && stepCadence > 0 {
		timer = time.NewTicker(stepCadence)
		defer timer.Stop()

		for {
			select {
			case <-se.ctx.Done():
				return
			case <-timer.C:
				se.checkAndEmitSignal()
			case <-time.After(chanWaitTime):
				slog.Warn("Strategy engine timeout while waiting for checkAndEmitSignal")
			}
		}
	} else {
		for {
			// per-event signal checking, or historical data
			select {
			case <-se.ctx.Done():
				return
			case aggregatorName, ok := <-se.newDataReceivedChannel:
				if !ok {
					slog.Warn("not ok in new data received channel")
					return
				}
				if general.ChannelAtLoadLevel[string](se.newDataReceivedChannel, 0.8) {
					slog.Warn("Strategy engine newDataReceivedChannel load warning", "aggregator", aggregatorName)
				}
				_ = aggregatorName // TODO: reverse commenting if you want to see which aggregator is sending data
				//slog.Debug(fmt.Sprintf("Received new data from aggregator %s", aggregatorName))
				streamTime := se.getDataStreamTime()
				lastSignalTime := se.getLastSignalTime()
				shouldEmitSignal := stepCadence == 0 || // per-event signal checking
					(!isRealTime && streamTime.Sub(lastSignalTime) >= stepCadence) // historical data

				if shouldEmitSignal {
					se.checkAndEmitSignal()
				}
			case <-time.After(chanWaitTime):
				slog.Warn("Strategy engine timeout while checking and emitting signal")
			}
		}
	}
}

func (se *strategyEngine) checkAndEmitSignal() {
	aggregatorData := se.getAllAggregatorData()

	signalFunc := se.signalFuncSupplier.GetSignalFunc()
	signalFuncStart := time.Now()
	signal := signalFunc(aggregatorData)
	signalFuncDuration := time.Since(signalFuncStart)

	if signalFuncDuration > 10*time.Millisecond {
		slog.Warn("Strategy engine signal function took too long", "duration", signalFuncDuration)
	}

	if signal != nil {
		signal.StrategyId = se.GetName()
		signal.SignalId = uuid.New().String()
		se.broadcastSignal(signal)
	}

	// write metrics, but suppress for now to investigate timeouts
	// if se.metricsWriter != nil {
	// 	metricValue, err := json.Marshal(signal)
	// 	if err != nil {
	// 		slog.Error("Error marshalling signal to metrics", "error", err)
	// 		return
	// 	}
	// 	signalMetric := datamodels.Metric{
	// 		MetricGeneratorId:   se.id,
	// 		MetricGeneratorName: se.GetName(),
	// 		MetricTime:          signal.StreamTime,
	// 		MetricGeneratorType: datamodels.MetricGeneratorTypeStrategy,
	// 		MetricName:          "signal",
	// 		MetricValue:         metricValue,
	// 	}
	// 	se.metricsWriter.Write(context.Background(), signalMetric)
	// }
}

func (se *strategyEngine) Stop() error {
	aggregators := se.getAggregators()
	subscribers := se.GetSubscribers()

	if len(aggregators) == 0 && len(subscribers) == 0 {
		slog.Warn("No aggregators or subscribers, skipping stop")
		return nil
	}

	if len(aggregators) == 0 {
		slog.Warn("No aggregators, skipping unsubscribe")
	} else {
		// Unsubscribe from all aggregators
		for name := range aggregators {
			se.setAggregatorBuffer(name, []datamodels.DataPoint{})
			err := se.unsubscribeFromAggregator(name)
			if err != nil {
				return err
			}
		}
	}

	if len(subscribers) == 0 {
		slog.Warn("No subscribers, skipping close")
	} else {
		for _, subscription := range subscribers {
			close(subscription.SignalChan)
			close(subscription.ErrorChan)
			close(subscription.DoneChan)
		}
	}

	slog.Info("Strategy engine stopped", "name", se.GetName())
	return nil
}

func (se *strategyEngine) IsStarted() bool {
	return se.ctx != nil && se.ctx.Err() == nil
}

func (se *strategyEngine) Subscribe(ctx context.Context, subscriberName string) (StrategySubscription, error) {
	// usually a portfolio, which will consume the signal and send feedback, so a reverse subscription is part of the subscribe

	if subscriberName == "" {
		return StrategySubscription{}, errors.New("subscriber name cannot be empty")
	}

	se.mutex.Lock()
	defer se.mutex.Unlock()

	subscriptionId := uuid.New().String()
	subscription := StrategySubscription{
		SubscriptionName: subscriberName,
		SubscriptionId:   subscriptionId,
		SignalChan:       make(chan datamodels.Signal, 100),
		ReadyChan:        make(chan struct{}, 1),
		ErrorChan:        make(chan error, 100),
		DoneChan:         make(chan struct{}, 1),
		FeedbackChan:     se.signalRewardChannel,
	}
	se.subscribers[subscriptionId] = subscription
	return subscription, nil
}

func (se *strategyEngine) GetSubscribers() map[string]StrategySubscription {
	se.mutex.RLock()
	defer se.mutex.RUnlock()
	return se.subscribers
}

func (se *strategyEngine) Unsubscribe(ctx context.Context, subscriptionId string) error {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	delete(se.subscribers, subscriptionId)
	slog.Info("Unsubscribed from strategy", "subscriptionId", subscriptionId)
	// check if channels are open, close if so
	if se.subscribers[subscriptionId].SignalChan != nil {
		slog.Info("Closing signal channel")
		close(se.subscribers[subscriptionId].SignalChan)
	}
	if se.subscribers[subscriptionId].ErrorChan != nil {
		slog.Info("Closing error channel")
		close(se.subscribers[subscriptionId].ErrorChan)
	}
	if se.subscribers[subscriptionId].DoneChan != nil {
		slog.Info("Closing done channel")
		close(se.subscribers[subscriptionId].DoneChan)
	}

	return nil
}

func (se *strategyEngine) processSignalRewardChannel() {

	// for now, this is a single channel shared by all subscribers
	// TODO: consider a channel per subscriber, because we might wish to separate feedback by subscriber
	// however, it seems unlikely that a single engine will be used to manage multiple portfolios, so we'll stick with single channel for now
	slog.Info("Strategy engine processSignalRewardChannel started")
	for {
		select {
		case <-se.ctx.Done():
			slog.Info("Strategy context done, stopping processSignalRewardChannel")
			return
		case feedback, ok := <-se.signalRewardChannel:
			if !ok {
				slog.Warn("Signal reward channel closed, stopping processSignalRewardChannel")
				return
			}

			slog.Debug("Strategy received feedback", "feedback", feedback)
			// link feedback to signal using buffer
			slog.Debug("Strategy calculated reward for signal", "signalId", feedback.SignalId, "reward", feedback.Reward)
			// making this into a go routine seems to increase throughput
			go se.rewardBuffer.AddElement(&feedback)

		case <-time.After(chanWaitTime):
			slog.Warn("Strategy engine timeout waiting for subscriber to receive signal reward feedback")
		}
	}
}

func (se *strategyEngine) broadcastReady() {
	subscribers := se.GetSubscribers()
	for _, subscription := range subscribers {
		if subscription.ReadyChan != nil {
			select {
			case subscription.ReadyChan <- struct{}{}:
			default:
				slog.Warn("Ready channel is full, dropping ready signal", "strategyName", se.GetName())
			}
		} else {
			slog.Error("Ready channel is nil, cannot send ready signal", "strategyName", se.GetName())
		}
	}
}

func (se *strategyEngine) broadcastSignal(signal *datamodels.Signal) {
	se.setLastSignalTime(signal.StreamTime)
	se.addSignalToBuffer(signal)
	subscribers := se.GetSubscribers()
	strategyName := se.GetName()
	for _, subscription := range subscribers {
		if subscription.SignalChan != nil {
			select {
			case subscription.SignalChan <- *signal:
				if general.ChannelAtLoadLevel[datamodels.Signal](subscription.SignalChan, 0.8) {
					slog.Warn("Strategy engine outgoing signal channel load warning", "subscriber", subscription.SubscriptionName)
				}
				slog.Debug("Broadcasted Signal", "signal", signal)
				continue
			case <-time.After(chanWaitTime):
				slog.Warn("Timeout waiting for subscriber to receive signal",
					"strategyName", strategyName,
					"signal", signal)
			}
		} else {
			slog.Error("Signal channel is nil, cannot send signal",
				"strategyName", strategyName,
				"signal", signal)
		}
	}
}

func (se *strategyEngine) broadcastError(err error) {
	strategyName := se.GetName()
	subscribers := se.GetSubscribers()
	for _, subscription := range subscribers {
		if subscription.ErrorChan != nil {
			select {
			case subscription.ErrorChan <- err:
			default:
				slog.Warn("Error channel is full, dropping error",
					"strategyName", strategyName,
					"error", err)
			}
		} else {
			slog.Error("Error channel is nil, cannot send error",
				"strategyName", strategyName,
				"error", err)
		}
	}
}

func (se *strategyEngine) broadcastDone() {
	strategyName := se.GetName()
	subscribers := se.GetSubscribers()
	for _, subscription := range subscribers {
		if subscription.DoneChan != nil {
			select {
			case subscription.DoneChan <- struct{}{}:
			default:
				slog.Warn("Done channel is full, dropping done signal",
					"strategyName", strategyName)
			}
		} else {
			slog.Error("Done channel is nil, cannot send done signal",
				"strategyName", strategyName)
		}
	}
}

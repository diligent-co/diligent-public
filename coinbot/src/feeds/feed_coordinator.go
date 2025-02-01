package feeds

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const defaultInterReadDelay = time.Millisecond * 10

type FeedReadySignal struct {
	feedName string
}

type FeedDoneSignal struct {
	feedName string
}

type FeedCoordinator struct {
	feeds                 map[string]*CsvFeed
	mutex                 sync.RWMutex
	currentTime           time.Time
	tickInterval          time.Duration
	interReadDelay        time.Duration
	nextValidReadTime     time.Time // used to track waiting for interReadDelay
	readyFeeds            map[string]bool
	readyChannel          chan FeedReadySignal
	doneChannel           chan FeedDoneSignal
	outboundReadyChannels map[string]chan struct{}
	started               bool
}

func NewFeedCoordinator(ctx context.Context, tickInterval time.Duration) *FeedCoordinator {
	fc := &FeedCoordinator{
		feeds:                 make(map[string]*CsvFeed),
		readyFeeds:            make(map[string]bool),
		readyChannel:          make(chan FeedReadySignal, 100),
		doneChannel:           make(chan FeedDoneSignal, 100),
		outboundReadyChannels: make(map[string]chan struct{}),
		tickInterval:          tickInterval,
		interReadDelay:        defaultInterReadDelay,
		started:               false,
		currentTime:           time.Time{},
	}
	go fc.coordinateFeeds(ctx)
	return fc
}

func (fc *FeedCoordinator) WithInterReadDelay(delay time.Duration) *FeedCoordinator {
	fc.interReadDelay = delay
	return fc
}

func (fc *FeedCoordinator) AddFeed(feed *CsvFeed) error {
	startTime := fc.GetStreamTime()
	if startTime.IsZero() || feed.startTime.Before(startTime) {
		startTime = feed.startTime
		fc.setStreamTime(startTime)
	}
	fc.mutex.Lock()
	defer fc.mutex.Unlock()

	if fc.started {
		return fmt.Errorf("cannot add feed after coordinator has started")
	}

	fc.feeds[feed.GetName()] = feed
	fc.readyFeeds[feed.GetName()] = false
	feed.coordinator = fc
	feed.coordinatorReadyChannel = make(chan struct{})
	fc.outboundReadyChannels[feed.GetName()] = feed.coordinatorReadyChannel
	return nil
}

func (fc *FeedCoordinator) getReadyFeeds() map[string]bool {
	fc.mutex.RLock()
	defer fc.mutex.RUnlock()

	readyFeeds := make(map[string]bool)
	for name, ready := range fc.readyFeeds {
		readyFeeds[name] = ready
	}

	return readyFeeds
}

func (fc *FeedCoordinator) removeFeed(feedName string) {
	fc.mutex.Lock()
	defer fc.mutex.Unlock()

	delete(fc.readyFeeds, feedName)
	delete(fc.outboundReadyChannels, feedName)
}

func (fc *FeedCoordinator) setFeedReady(feedName string) {
	fc.mutex.Lock()
	defer fc.mutex.Unlock()

	fc.readyFeeds[feedName] = true
}

func (fc *FeedCoordinator) setFeedNotReady(feedName string) {
	fc.mutex.Lock()
	defer fc.mutex.Unlock()

	fc.readyFeeds[feedName] = false
}

func (fc *FeedCoordinator) broadcastStepComplete() {
	fc.mutex.Lock()
	defer fc.mutex.Unlock()
	for _, channel := range fc.outboundReadyChannels {
		channel <- struct{}{}
	}
}

func (fc *FeedCoordinator) GetStreamTime() time.Time {
	fc.mutex.RLock()
	defer fc.mutex.RUnlock()

	return fc.currentTime
}

func (fc *FeedCoordinator) setStreamTime(time time.Time) {
	fc.mutex.Lock()
	defer fc.mutex.Unlock()

	fc.currentTime = time
}

func (fc *FeedCoordinator) coordinateFeeds(ctx context.Context) {
	time.Sleep(time.Millisecond * 500)
	for {
		select {
		case <-ctx.Done():
			return
		case feedReady, ok := <-fc.readyChannel:
			if !ok {
				slog.Error("FeedCoordinator readyChannel closed")
				return
			}
			fc.setFeedReady(feedReady.feedName)
			slog.Debug("FeedCoordinator ready signal received", "feedName", feedReady.feedName)
			fc.checkReadyAndStep()
		case feedDone, ok := <-fc.doneChannel:
			if !ok {
				slog.Error("FeedCoordinator doneChannel closed")
				return
			}
			slog.Info("FeedCoordinator done signal received", "feedName", feedDone.feedName)
			fc.removeFeed(feedDone.feedName)
			fc.checkReadyAndStep()
		}
	}
}

func (fc *FeedCoordinator) checkReadyAndStep() {
	// Check if all feeds are ready
	allReady := true
	readyFeeds := fc.getReadyFeeds()
	for _, ready := range readyFeeds {
		if !ready {
			allReady = false
			break
		}
	}

	if allReady {
		if time.Now().Before(fc.nextValidReadTime) {
			waitTime := fc.nextValidReadTime.Sub(time.Now())
			time.Sleep(waitTime)
		}
		fc.nextValidReadTime = time.Now().Add(fc.interReadDelay)
		// Too late for newcomers to join
		fc.started = true
		// Advance time
		nextTime := fc.GetStreamTime().Add(fc.tickInterval)
		fc.setStreamTime(nextTime)

		// Reset ready states
		for name := range fc.readyFeeds {
			fc.setFeedNotReady(name)
		}

		// Signal feeds with new time
		fc.broadcastStepComplete()
	}
}

// func (fc *FeedCoordinator) coordinateFeedDone(ctx context.Context) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case feedDone, ok := <-fc.doneChannel:
// 			if !ok {
// 				slog.Error("FeedCoordinator doneChannel closed")
// 				return
// 			}
// 			// remove feed from readyFeeds
// 			slog.Info("FeedCoordinator done signal received", "feedName", feedDone.feedName)
//             fc.removeFeed(feedDone.feedName)
// 		}
// 	}
// }

func (fc *FeedCoordinator) GetTickInterval() time.Duration {
	fc.mutex.RLock()
	defer fc.mutex.RUnlock()
	return fc.tickInterval
}

func (fc *FeedCoordinator) SendFeedReadySignal(feedName string) {
	fc.readyChannel <- FeedReadySignal{feedName: feedName}
}

func (fc *FeedCoordinator) SendFeedDoneSignal(feedName string) {
	fc.doneChannel <- FeedDoneSignal{feedName: feedName}
}

func (fc *FeedCoordinator) Stop() error {
	fc.mutex.Lock()
	defer fc.mutex.Unlock()

	if !fc.started {
		return fmt.Errorf("coordinator not started")
	}

	for _, feed := range fc.feeds {
		if err := feed.Stop(); err != nil {
			return fmt.Errorf("failed to stop feed %s: %w", feed.GetName(), err)
		}
	}

	fc.started = false
	return nil
}

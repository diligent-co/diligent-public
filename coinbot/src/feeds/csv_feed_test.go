package feeds

import (
	"bytes"
	"coinbot/src/config"
	"coinbot/src/datamodels"
	"coinbot/src/utils/symbols"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func GetEthPathAndSchema() (string, *datamodels.CsvWithTimestampSchema) {
	schema := &datamodels.CsvWithTimestampSchema{
		RelationName:       "ETHUSD",
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt, DefaultValue: 0},
			{ColumnIndex: 1, FieldName: "price_usd", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
			{ColumnIndex: 2, FieldName: "volume_eth", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
		},
	}

	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	return thisDir + "/../../test/data/ETHUSD.csv", schema
}

func GetBtcPathAndSchema() (string, *datamodels.CsvWithTimestampSchema) {
	schema := &datamodels.CsvWithTimestampSchema{
		RelationName:       "BTCUSD",
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt, DefaultValue: 0},
			{ColumnIndex: 1, FieldName: "price_usd", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
			{ColumnIndex: 2, FieldName: "volume_btc", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
		},
	}

	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	return thisDir + "/../../test/data/XBTUSD.csv", schema
}

type KrakenCsvFeedTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	symbolDict *symbols.SymbolsDictionary
}

func GetEthFilepath() string {
	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	return thisDir + "/../../test/data/ETHUSD.csv"
}

func GetEthSchema() *datamodels.CsvWithTimestampSchema {
	schema := &datamodels.CsvWithTimestampSchema{
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
			{
				ColumnIndex:  0,
				FieldName:    "timestamp",
				FieldType:    datamodels.FieldTypeInt,
				DefaultValue: 0,
			},
			{
				ColumnIndex:  1,
				FieldName:    "price_usd",
				FieldType:    datamodels.FieldTypeFloat,
				DefaultValue: 0.0,
			},
			{
				ColumnIndex:  2,
				FieldName:    "volume_eth",
				FieldType:    datamodels.FieldTypeFloat,
				DefaultValue: 0.0,
			},
		},
	}
	return schema
}

func GetLogBuffer() *bytes.Buffer {
	var buffer bytes.Buffer
	log.SetOutput(&buffer)
	return &buffer
}

func ResetLogBuffer(buffer *bytes.Buffer) {
	log.SetOutput(os.Stdout)
	buffer.Reset()
}

func TestKrakenCsvFeedSuite(t *testing.T) {
	suite.Run(t, new(KrakenCsvFeedTestSuite))
}

func (s *KrakenCsvFeedTestSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	config, err := config.Load()
	s.Require().NoError(err)
	s.symbolDict = symbols.NewSymbolsDictionaryFromConfig(&config.SymbolsConfig)
}

func (s *KrakenCsvFeedTestSuite) TearDownTest() {
	s.cancel()
}

func (s *KrakenCsvFeedTestSuite) TestBasicFeedOperation() {
	// Setup
	filePath := GetEthFilepath()
	schema := GetEthSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)

	feed, err := NewCsvFeedBuilder(filePath).
		WithStartTime(startTime).
		WithSchema(schema).
		WithHasHeader(false).
		Build()
	s.Require().NoError(err)
	s.Require().NotNil(feed)

	coordinator := NewFeedCoordinator(s.ctx, time.Minute)
	coordinator.AddFeed(feed)

	// Subscribe to feed
	csvFeedSubscription, err := feed.Subscribe(s.ctx, "test")
	s.Require().NoError(err)
	s.Require().NotNil(csvFeedSubscription)

	// Start feed
	err = feed.Start(s.ctx)
	s.Require().NoError(err)

	// Collect data points for 5 minutes
	receivedDpCount := 0
	receivedDataPoints := make([]datamodels.DataPoint, 0)
	prices := make([]float64, 0)
	timeout := time.After(10 * time.Second)

	for {
		select {
		case _, ok := <-csvFeedSubscription.DoneChan:
			if ok {
				// wait for one second before checking results
				s.T().Log("Done signal received")
				goto checkResults
			}

		case dp := <-csvFeedSubscription.DataPointChan:
			receivedDpCount++
			receivedDataPoints = append(receivedDataPoints, dp)
			s.Require().NoError(err)
			priceElement, err := dp.Get("price_usd")
			s.Require().NoError(err)
			priceVal, err := priceElement.GetTypedValue()
			s.Require().NoError(err)
			prices = append(prices, priceVal.(float64))
		case <-timeout:
			s.T().Logf("Timeout reached, received %d data points", len(receivedDataPoints))
			goto checkResults
		case <-s.ctx.Done():
			s.T().Logf("Context done, received %d data points", len(receivedDataPoints))
			goto checkResults
		}
	}

checkResults:
	// Verify results
	s.T().Logf("Received %d data points", receivedDpCount)
	s.T().Logf("Collected %d data points", len(receivedDataPoints))
	s.T().Logf("Received %d prices", len(prices))
	// should have received exactly 1000 datapoints and prices
	s.Equal(1000, len(receivedDataPoints))
	s.Equal(1000, len(prices))

	// Verify all prices are non-zero
	for i, price := range prices {
		s.Greater(price, 0.0, "Price at index %d should be greater than 0", i)
	}

	// Clean up
	err = feed.Stop()
	s.Require().NoError(err)
}

func (s *KrakenCsvFeedTestSuite) TestGetDataBetween() {
	filePath := GetEthFilepath()
	schema := GetEthSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)
	feed, err := NewCsvFeedBuilder(filePath).
		WithStartTime(startTime).
		WithSchema(schema).
		WithHasHeader(false).
		Build()
	s.Require().NoError(err)

	// Request 15 minutes of data
	endTime := startTime.Add(15 * time.Minute)

	dataPoints, err := feed.GetDataBetween(s.ctx, startTime, endTime)
	if err != nil {
		s.T().Logf("Error getting data between %s and %s: %s", startTime, endTime, err)
	}
	s.Require().NoError(err)
	s.NotEmpty(dataPoints)

	// Verify timestamps are within range
	for _, dp := range dataPoints {
		timestamp := dp.GetTimestamp()
		s.True(timestamp.After(startTime) || timestamp.Equal(startTime))
		s.True(timestamp.Before(endTime) || timestamp.Equal(endTime))
	}
}

func (s *KrakenCsvFeedTestSuite) TestSubscriptionManagement() {
	filePath := GetEthFilepath()
	schema := GetEthSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)
	feed, err := NewCsvFeedBuilder(filePath).
		WithStartTime(startTime).
		WithSchema(schema).
		WithHasHeader(false).
		Build()
	s.Require().NoError(err)

	// Create multiple subscriptions
	sub1, err := feed.Subscribe(s.ctx, "test1")
	s.Require().NoError(err)
	sub2, err := feed.Subscribe(s.ctx, "test2")
	s.Require().NoError(err)

	// Verify subscriptions
	s.Require().NotNil(sub1)
	s.Require().NotNil(sub2)
	s.NotEqual(sub1.SubscriptionId, sub2.SubscriptionId)

	// Unsubscribe and verify
	err = feed.Unsubscribe(s.ctx, sub1.SubscriptionId)
	s.Require().NoError(err)

	// Clean up
	err = feed.Stop()
	s.Require().NoError(err)
}

func (s *KrakenCsvFeedTestSuite) TestFeedRestart() {
	filePath := GetEthFilepath()
	schema := GetEthSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)
	feed, err := NewCsvFeedBuilder(filePath).
		WithStartTime(startTime).
		WithSchema(schema).
		WithHasHeader(false).
		Build()
	s.Require().NoError(err)

	coordinator := NewFeedCoordinator(s.ctx, time.Minute)
	coordinator.AddFeed(feed)

	// First start
	err = feed.Start(s.ctx)
	s.Require().NoError(err)

	// Try to start again
	err = feed.Start(s.ctx)
	s.Error(err, "Should not be able to start an already started feed")

	// Stop
	err = feed.Stop()
	s.Require().NoError(err)

	// Try to start again, should fail
	err = feed.Start(s.ctx)
	s.Require().Error(err)

	// Clean up
	err = feed.Stop()
	s.Require().NoError(err)
}

func (s *KrakenCsvFeedTestSuite) TestDoneSignalPropagation() {
	buffer := GetLogBuffer()
	defer ResetLogBuffer(buffer)

	// Setup
	filePath := GetEthFilepath()
	schema := GetEthSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)

	feed, err := NewCsvFeedBuilder(filePath).
		WithStartTime(startTime).
		WithSchema(schema).
		WithHasHeader(false).
		Build()
	s.Require().NoError(err)
	s.Require().NotNil(feed)

	coordinator := NewFeedCoordinator(s.ctx, time.Minute)
	coordinator.AddFeed(feed)

	// Create multiple subscribers to verify done signal reaches all
	sub1, err := feed.Subscribe(s.ctx, "test_subscriber_1")
	s.Require().NoError(err)
	sub2, err := feed.Subscribe(s.ctx, "test_subscriber_2")
	s.Require().NoError(err)

	// Create channels to track done signals
	subsDone := make(chan struct{})

	// Track data points and done signals for both subscribers
	sub1DataPointCount := 0
	sub2DataPointCount := 0

	go listenToSubscription(sub1, subsDone, &sub1DataPointCount)
	go listenToSubscription(sub2, subsDone, &sub2DataPointCount)

	// Start feed
	err = feed.Start(s.ctx)
	s.Require().NoError(err)

	// Wait for both subscribers to receive done signals or timeout
	subsWaiting := 2
	for subsWaiting > 0 {
		select {
		case <-subsDone:
			fmt.Println("Subscriber done signal received")
			subsWaiting--
		case <-time.After(10 * time.Second):
			s.T().Logf("Timeout waiting for done signals, %d subscribers still waiting", subsWaiting)
			s.Fail("Timeout waiting for done signals")
			return
		}
	}

	s.Equal(sub1DataPointCount, 1000)
	s.Equal(sub2DataPointCount, 1000)
	// Verify feed state after done signals

	feedStopErr := feed.Stop()
	s.Require().NoError(feedStopErr)

	s.T().Logf("Log buffer: %s", buffer.String())
}

func listenToSubscription(subscription *datamodels.DataPointSubscription,
	subsDone chan struct{}, dataPointCount *int) {
	for {
		select {
		case err := <-subscription.ErrorChan:
			fmt.Printf("Subscriber received error: %s\n", err)
			return
		case <-subscription.DoneChan:
			fmt.Println("Subscriber received done signal")
			subsDone <- struct{}{}
			time.Sleep(100 * time.Millisecond)
			return
		case <-subscription.DataPointChan:
			*dataPointCount++
		case <-time.After(5 * time.Second):
			fmt.Println("Subscriber timeout")
			return
		}
	}
}

func (s *KrakenCsvFeedTestSuite) TestChannelListening() {

	myChan := make(chan int)

	go func() {
		myChan <- 1
		myChan <- 2
		myChan <- 3
		close(myChan)
	}()
	expectedVal := 1

	defer s.T().Log("Test over")
	stopLoop := false
	for !stopLoop {
		select {
		case val, ok := <-myChan:
			if !ok {
				s.T().Logf("Channel closed, with value %d", val)
				stopLoop = true
				s.T().Log("Setting stopLoop to true")
				break
			}
			s.Equal(val, expectedVal)
			expectedVal++
		case <-time.After(1 * time.Second):
			s.Fail("Timeout waiting for value on channel")
		}
		s.T().Log("Out of select, still in for")
	}
	s.T().Log("Testing printing past the for loop")
}

func TestCoordinatedCsvFeeds(t *testing.T) {
	// Setup test data files
	tempDir := t.TempDir()

	// Create test CSV files
	feed1Data := []string{
		"timestamp,price,volume",
		"1609459200,100,1.0", // 2021-01-01 00:00:00
		"1609459260,101,1.1", // 2021-01-01 00:01:00
		"1609459320,102,1.2", // 2021-01-01 00:02:00
	}
	feed2Data := []string{
		"timestamp,bid,ask",
		"1609459200,99,101",  // 2021-01-01 00:00:00
		"1609459260,100,102", // 2021-01-01 00:01:00
		"1609459320,101,103", // 2021-01-01 00:02:00
	}

	feed1Path := filepath.Join(tempDir, "feed1.csv")
	feed2Path := filepath.Join(tempDir, "feed2.csv")

	writeErr := os.WriteFile(feed1Path, []byte(strings.Join(feed1Data, "\n")), 0644)
	if writeErr != nil {
		t.Fatalf("Failed to write test data to feed1: %v", writeErr)
	}
	writeErr = os.WriteFile(feed2Path, []byte(strings.Join(feed2Data, "\n")), 0644)
	if writeErr != nil {
		t.Fatalf("Failed to write test data to feed2: %v", writeErr)
	}

	// Create schemas
	feed1Schema := &datamodels.CsvWithTimestampSchema{
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt},
			{ColumnIndex: 1, FieldName: "price", FieldType: datamodels.FieldTypeFloat},
			{ColumnIndex: 2, FieldName: "volume", FieldType: datamodels.FieldTypeFloat},
		},
		TimestampFieldName: "timestamp",
		RelationName:       "feed1",
	}

	feed2Schema := &datamodels.CsvWithTimestampSchema{
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt},
			{ColumnIndex: 1, FieldName: "bid", FieldType: datamodels.FieldTypeFloat},
			{ColumnIndex: 2, FieldName: "ask", FieldType: datamodels.FieldTypeFloat},
		},
		TimestampFieldName: "timestamp",
		RelationName:       "feed2",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create coordinator first
	coordinator := NewFeedCoordinator(ctx, time.Minute)

	// Create feeds
	startTime := time.Unix(1609459200, 0) // 2021-01-01 00:00:00
	endTime := time.Unix(1609459321, 0)   // 2021-01-01 00:02:00

	feed1, err := NewCsvFeedBuilder(feed1Path).
		WithStartTime(startTime).
		WithEndTime(endTime).
		WithSchema(feed1Schema).
		WithHasHeader(true).
		Build()

	if err != nil {
		t.Fatalf("Failed to create feed1: %v", err)
	}

	feed2, err := NewCsvFeedBuilder(feed2Path).
		WithStartTime(startTime).
		WithEndTime(endTime).
		WithSchema(feed2Schema).
		WithHasHeader(true).
		Build()

	if err != nil {
		t.Fatalf("Failed to create feed2: %v", err)
	}

	// Add feeds to coordinator
	if err := coordinator.AddFeed(feed1); err != nil {
		t.Fatalf("Failed to add feed1 to coordinator: %v", err)
	}
	if err := coordinator.AddFeed(feed2); err != nil {
		t.Fatalf("Failed to add feed2 to coordinator: %v", err)
	}

	// Subscribe to feeds
	sub1, err := feed1.Subscribe(ctx, "test_sub_1")
	if err != nil {
		t.Fatalf("Failed to subscribe to feed1: %v", err)
	}

	sub2, err := feed2.Subscribe(ctx, "test_sub_2")
	if err != nil {
		t.Fatalf("Failed to subscribe to feed2: %v", err)
	}

	t.Logf("Starting feeds...")
	err = feed1.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start Feed1: %v", err)
	}
	feed2.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start Feed2: %v", err)
	}
	t.Logf("Feeds started.")

	type timestampedData struct {
		timestamp time.Time
		dataPoint datamodels.DataPoint
	}

	feed1DataPoints := make([]timestampedData, 0)
	feed2DataPoints := make([]timestampedData, 0)

	// Collect data in goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case dp, ok := <-sub1.DataPointChan:
				if !ok {
					t.Log("Feed1 channel closed")
					return
				}
				t.Logf("Feed1 data point received: %d", dp.GetTimestamp().Unix())
				feed1DataPoints = append(feed1DataPoints, timestampedData{
					timestamp: time.Now(),
					dataPoint: dp,
				})
			case <-sub1.DoneChan:
				t.Log("Feed1 done signal received")
				return
			case err := <-sub1.ErrorChan:
				t.Errorf("Feed1 error: %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case dp, ok := <-sub2.DataPointChan:
				if !ok {
					t.Log("Feed2 channel closed")
					return
				}
				t.Logf("Feed2 data point received: %d", dp.GetTimestamp().Unix())
				feed2DataPoints = append(feed2DataPoints, timestampedData{
					timestamp: time.Now(),
					dataPoint: dp,
				})
			case <-sub2.DoneChan:
				t.Log("Feed2 done signal received")
				return
			case err := <-sub2.ErrorChan:
				t.Errorf("Feed2 error: %v", err)
				return
			}
		}
	}()

	doneChan := make(chan struct{})
	go func() {
		t.Log("Waiting for data collection to complete")
		wg.Wait()
		doneChan <- struct{}{}
	}()

	select {
	case <-doneChan:
		break
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for data collection to complete")
	}

	// Verify results
	if len(feed1DataPoints) != 3 {
		t.Errorf("Expected 3 data points from feed1, got %d", len(feed1DataPoints))
	}
	if len(feed2DataPoints) != 3 {
		t.Errorf("Expected 3 data points from feed2, got %d", len(feed2DataPoints))
	}

	// Verify timestamps match between feeds and data arrives close together
	for i := 0; i < len(feed1DataPoints); i++ {
		// Verify data point timestamps match
		if !feed1DataPoints[i].dataPoint.GetTimestamp().Equal(feed2DataPoints[i].dataPoint.GetTimestamp()) {
			t.Errorf("Data point timestamp mismatch at index %d: feed1=%v, feed2=%v",
				i, feed1DataPoints[i].dataPoint.GetTimestamp(), feed2DataPoints[i].dataPoint.GetTimestamp())
		}

		// Verify arrival times are within 100ms of each other
		arrivalDiff := feed1DataPoints[i].timestamp.Sub(feed2DataPoints[i].timestamp)
		if arrivalDiff < -100*time.Millisecond || arrivalDiff > 100*time.Millisecond {
			t.Errorf("Data points arrived too far apart at index %d: feed1=%v, feed2=%v, diff=%v",
				i, feed1DataPoints[i].timestamp, feed2DataPoints[i].timestamp, arrivalDiff)
		}
	}

	// Add cleanup
	if err := coordinator.Stop(); err != nil {
		t.Errorf("Failed to stop coordinator: %v", err)
	}
}

func TestTwoFeeds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ethPath, ethSchema := GetEthPathAndSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)
	tickInterval := time.Minute

	ethFeed, err := NewCsvFeedBuilder(ethPath).
		WithStartTime(startTime).
		WithSchema(ethSchema).
		Build()
	if err != nil {
		t.Fatalf("Failed to create ETH feed: %v", err)
	}

	// Setup second CSV feed (BTC/USD) - using same file for test simplicity
	btcPath, btcSchema := GetBtcPathAndSchema()
	btcFeed, err := NewCsvFeedBuilder(btcPath).
		WithStartTime(startTime).
		WithSchema(btcSchema).
		Build()
	if err != nil {
		t.Fatalf("Failed to create BTC feed: %v", err)
	}

	fc := NewFeedCoordinator(ctx, tickInterval)
	fc.AddFeed(ethFeed)
	fc.AddFeed(btcFeed)

	ethSub, err := ethFeed.Subscribe(ctx, "eth_sub")
	if err != nil {
		t.Fatalf("Failed to subscribe to ETH feed: %v", err)
	}

	btcSub, err := btcFeed.Subscribe(ctx, "btc_sub")
	if err != nil {
		t.Fatalf("Failed to subscribe to BTC feed: %v", err)
	}

	ethData := make([]datamodels.DataPoint, 0)
	btcData := make([]datamodels.DataPoint, 0)

	errs := make([]error, 0)

	doneChan := make(chan struct{})

	// Create listener methods
	go func() {
		for {
			select {
			case dp := <-ethSub.DataPointChan:
				ethData = append(ethData, dp)
			case <-ethSub.DoneChan:
				t.Log("ETH feed done signal received")
				doneChan <- struct{}{}
				return
			case err := <-ethSub.ErrorChan:
				t.Errorf("ETH feed error: %v", err)
				errs = append(errs, err)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case dp := <-btcSub.DataPointChan:
				btcData = append(btcData, dp)
			case <-btcSub.DoneChan:
				t.Log("BTC feed done signal received")
				doneChan <- struct{}{}
				return
			case err := <-btcSub.ErrorChan:
				t.Errorf("BTC feed error: %v", err)
				errs = append(errs, err)
				return
			}
		}
	}()

	ethFeed.Start(ctx)
	btcFeed.Start(ctx)

	// Wait for 2 done signals
	<-doneChan
	<-doneChan

	// Check for errors
	if len(errs) > 0 {
		t.Fatalf("Errors received: %v", errs)
	}
}

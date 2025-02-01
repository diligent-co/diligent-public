//go:build unit

package aggregators

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"coinbot/src/config"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
	"coinbot/src/utils/symbols"

	"github.com/stretchr/testify/suite"
)

type HybridAggregatorTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	symbolDict *symbols.SymbolsDictionary
}

func GetFilePath() string {
	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	return thisDir + "/../../test/data/ETHUSD.csv"
}

func GetSchema() *datamodels.CsvWithTimestampSchema {
	return &datamodels.CsvWithTimestampSchema{
		RelationName:       "ETHUSD",
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt, DefaultValue: 0},
			{ColumnIndex: 1, FieldName: "price_usd", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
			{ColumnIndex: 2, FieldName: "volume_eth", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
		},
	}
}

func TestHybridAggregatorSuite(t *testing.T) {
	suite.Run(t, new(HybridAggregatorTestSuite))
}

func (s *HybridAggregatorTestSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	config, err := config.Load()
	s.Require().NoError(err)
	s.symbolDict = symbols.NewSymbolsDictionaryFromConfig(&config.SymbolsConfig)
}

func (s *HybridAggregatorTestSuite) TearDownTest() {
	s.cancel()
}

func (s *HybridAggregatorTestSuite) TestBasicFeedOperation() {
	// Setup CSV feed
	ctx := context.Background()
	filePath := GetFilePath()
	schema := GetSchema()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)
	tickInterval := time.Minute

	feed, err := feeds.NewCsvFeedBuilder(filePath).
		WithStartTime(startTime).
		WithSchema(schema).
		Build()
	s.Require().NoError(err)
	s.Require().NotNil(feed)
	fc := feeds.NewFeedCoordinator(ctx, tickInterval)
	fc.AddFeed(feed)

	// Setup price aggregator

	// Setup aggregator
	lookbackPeriod := time.Minute * 5
	updateCadence := time.Minute * 5
	fiveMinMovingAvgAggregator, err := NewHybridAggregator(nil,
		feed,
		lookbackPeriod,
		updateCadence,
		MeanFunc,
		[]string{"price_usd"},
		[]string{"mean_price_usd"})
	s.Require().NoError(err)
	s.Require().NotNil(fiveMinMovingAvgAggregator)

	// Subscribe to both feed and aggregator
	feedSub, err := feed.Subscribe(s.ctx, "test_feed_sub")
	s.Require().NoError(err)
	s.Require().NotNil(feedSub)

	aggSub, err := fiveMinMovingAvgAggregator.Subscribe(s.ctx, "test_agg_sub")
	s.Require().NoError(err)
	s.Require().NotNil(aggSub)



	// Collect data points for verification
	feedDps := make([]datamodels.DataPoint, 0)
	aggDps := make([]datamodels.DataPoint, 0)

	feedDoneTime := time.Time{}
	aggDoneTime := time.Time{}

	feedDone := false
	aggDone := false


	go func() {
		for {
			select {
			case dp, ok := <-feedSub.DataPointChan:
				if !ok {
					s.T().Log("Feed channel closed")
					feedDone = true
					return
				}
				feedDps = append(feedDps, dp)
			case <-time.After(time.Second * 1):
				s.T().Log("Feed channel timeout")
				feedDone = true
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case dp, ok := <-aggSub.DataPointChan:
				if !ok {
					s.T().Log("Aggregator channel closed")
					aggDone = true
					return
				}
				aggDps = append(aggDps, dp)
			case <-time.After(time.Second * 1):
				s.T().Log("Aggregator channel timeout")
				aggDone = true
				return
			}
		}
	}()

	go func() {
		for {
			if feedDone && aggDone {
				break
			}
			select {
			case <-feedSub.DoneChan:
				feedDone = true
			case <-aggSub.DoneChan:
				aggDone = true
			}
		}
	}()


	// starting aggregator should start the feed
	err = fiveMinMovingAvgAggregator.Start(s.ctx)
	s.Require().NoError(err)
	s.Require().True(feed.IsStarted())

	for !feedDone || !aggDone {
		time.Sleep(1 * time.Second)
	}

	goto checkResults

checkResults:
	s.T().Logf("Received %d feed data points and %d aggregator data points", len(feedDps), len(aggDps))

	// check that feed and aggregator are done
	//s.T().Logf("Time delta is %v", aggDoneTime.Sub(feedDoneTime).())
	s.Require().False(aggDoneTime.Before(feedDoneTime))
	// check that DataPointChan are closed for both feed and aggregator
	s.Require().False(len(feedSub.DataPointChan) > 0)
	if len(feedSub.DataPointChan) > 0 {
		s.T().Log("Feed channel is not empty")
	}
	if len(aggSub.DataPointChan) > 0 {
		s.T().Log("Aggregator channel is not empty")
	}
	s.Require().False(len(aggSub.DataPointChan) > 0)

	// should get 1000 datapoints from feed
	s.Require().Equal(1000, len(feedDps))

	// should have 1000+ aggregated datapoints
	s.Require().Greater(len(aggDps), 900)

	s.T().Log("Test passed")
	// Cleanup
	err = feed.Stop()
	s.Require().NoError(err)

	err = fiveMinMovingAvgAggregator.Stop()
	s.Require().NoError(err)
}

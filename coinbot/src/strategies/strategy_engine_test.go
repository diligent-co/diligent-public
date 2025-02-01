//go:build unit

package strategies

import (
	"coinbot/src/aggregators"
	"coinbot/src/config"
	"coinbot/src/datamodels"
	"coinbot/src/feeds"
	"coinbot/src/utils/symbols"
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type StrategyEngineTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	symbolDict *symbols.SymbolsDictionary
}

func GetFilePath1() string {
	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	return thisDir + "/../../test/data/ETHUSD.csv"
}

func GetFilePath2() string {
	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	return thisDir + "/../../test/data/XBTUSD.csv"
}

func GetSchema1() *datamodels.CsvWithTimestampSchema {
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

func GetSchema2() *datamodels.CsvWithTimestampSchema {
	return &datamodels.CsvWithTimestampSchema{
		RelationName:       "BTCUSD",
		TimestampFieldName: "timestamp",
		Fields: []datamodels.CsvField{
			{ColumnIndex: 0, FieldName: "timestamp", FieldType: datamodels.FieldTypeInt, DefaultValue: 0},
			{ColumnIndex: 1, FieldName: "price_usd", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
			{ColumnIndex: 2, FieldName: "volume_btc", FieldType: datamodels.FieldTypeFloat, DefaultValue: 0.0},
		},
	}
}

func GetSignalFunctionSupplier() SignalFunctionSupplier {
	return GetDummySignalSupplier([]string{"ETHUSD", "BTCUSD"})
}

func TestStrategyEngineSuite(t *testing.T) {
	suite.Run(t, new(StrategyEngineTestSuite))
}

func (s *StrategyEngineTestSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	config, err := config.Load()
	s.Require().NoError(err)
	s.symbolDict = symbols.NewSymbolsDictionaryFromConfig(&config.SymbolsConfig)
}

func (s *StrategyEngineTestSuite) TearDownTest() {
	s.cancel()
}

func (s *StrategyEngineTestSuite) TestBasicStrategyOperation() {
	// Setup first CSV feed (ETH/USD)
	filePath1 := GetFilePath1()
	schema1 := GetSchema1()
	startTime := time.Date(2021, 6, 17, 15, 32, 0, 0, time.UTC)
	tickInterval := time.Minute

	feed1, err := feeds.NewCsvFeedBuilder(filePath1).
		WithStartTime(startTime).
		WithSchema(schema1).
		Build()
	s.Require().NoError(err)
	s.Require().NotNil(feed1)

	// Setup second CSV feed (BTC/USD) - using same file for test simplicity
	filePath2 := GetFilePath2()
	schema2 := GetSchema2()
	feed2, err := feeds.NewCsvFeedBuilder(filePath2).
		WithStartTime(startTime).
		WithSchema(schema2).
		Build()
	s.Require().NoError(err)
	s.Require().NotNil(feed2)

	fc := feeds.NewFeedCoordinator(s.ctx, tickInterval)
	fc.AddFeed(feed1)
	fc.AddFeed(feed2)

	// Setup aggregators
	lookbackPeriod := time.Minute * 5
	minUpdateCadence := time.Minute * 1

	ethAggregator, err := aggregators.NewHybridAggregator(nil,
		feed1,
		lookbackPeriod,
		minUpdateCadence,
		aggregators.MeanFunc,
		[]string{"price_usd"},
		[]string{"eth_mean_price"})
	s.Require().NoError(err)
	s.Require().NotNil(ethAggregator)

	btcAggregator, err := aggregators.NewHybridAggregator(nil,
		feed2,
		lookbackPeriod,
		minUpdateCadence,
		aggregators.MeanFunc,
		[]string{"price_usd"},
		[]string{"btc_mean_price"})
	s.Require().NoError(err)
	s.Require().NotNil(btcAggregator)

	// Setup strategy
	strategy, buildErr := NewStratEngine().
		WithAggregator("eth", ethAggregator).
		WithAggregator("btc", btcAggregator).
		WithSignalFunctionSupplier(GetDummySignalSupplier([]string{"ETHUSD", "BTCUSD"})).
		WithBufferSize(1000).
		WithStepCadence(time.Minute).
		WithRealTime(false).
		Build()
	s.Require().NotNil(strategy)
	s.Require().NoError(buildErr)

	// Subscribe to strategy
	stratSub, err := strategy.Subscribe(s.ctx, "test_strat_sub")
	s.Require().NoError(err)
	s.Require().NotNil(stratSub)

	// Start strategy (which will start aggregators and feeds)
	err = strategy.Start(s.ctx)
	s.Require().NoError(err)

	// Collect signals for verification
	signals := make([]datamodels.Signal, 0)
	timeout := time.After(10 * time.Second)

	for {
		select {
		case signal, ok := <-stratSub.SignalChan:
			if !ok {
				s.T().Error("Signal channel closed, stopping strategy")
				goto checkResults
			}
			signals = append(signals, signal)
		case err, ok := <-stratSub.ErrorChan:
			if !ok {
				s.T().Error("Error channel closed, stopping strategy")
				goto checkResults
			}
			s.T().Errorf("Error received: %v", err)
		case <-stratSub.DoneChan:
			s.T().Log("Done received")
			goto checkResults
		case <-timeout:
			goto checkResults
		}
	}

checkResults:
	s.T().Logf("Received %d signals", len(signals))

	// Verify we received signals
	s.Greater(len(signals), 0, "Should have received some signals")

	// Verify signal contents
	for _, signal := range signals {
		s.Equal(datamodels.SignalTypeAllocation, signal.SignalType)
		s.Equal(0.5, signal.Allocations["ETH"])
		s.Equal(0.5, signal.Allocations["XBT"])
	}

	// Cleanup
	err = strategy.Stop()
	s.Require().NoError(err)
}

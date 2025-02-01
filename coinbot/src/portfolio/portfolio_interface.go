package portfolio

import (
	"context"

	///"reflect"
	"coinbot/src/aggregators"
	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/exchange"
	"coinbot/src/feeds"
	"coinbot/src/metrics"
	"coinbot/src/strategies"
	"coinbot/src/utils/errors"
	"coinbot/src/utils/general"
)

type Portfolio interface {
    // Core operations
    ExecuteOrder(order Order) error
    GetPosition(asset datamodels.Asset) (datamodels.Position, error)
    GetTotalValue() float64
    
    // Performance tracking
    GetTransactions() []datamodels.Transaction
    Start(ctx context.Context) error
    SubscribeToMetrics() chan datamodels.Metric
}

func SimulatedPortfolioFromConfig(config *datamodels.PortfolioConfig,
    priceAggregators map[datamodels.Asset]aggregators.Aggregator,
    strategyEngine strategies.StrategyEngine) (Portfolio, error) {

    metricsWriter, err := metrics.BuildMetricsWriter(config.MetricsWriter)
    if err != nil {
        return nil, err
    }

	portfolio, buildErr := NewSimulatedPortfolio().
		WithStrategies([]strategies.StrategyEngine{strategyEngine}).
		WithInitialBalance(config.InitialBalance).
		WithPriceAggregators(priceAggregators).
		WithAssets(config.Assets).
		WithMetricsWriter(metricsWriter).
		Build()

	if buildErr != nil {
		return nil, buildErr
	}

	return portfolio, nil
}

func BuildPortfolioFromConfig(ctx context.Context, 
    portfolioConfig *datamodels.PortfolioConfig, 
    db database.AggFeedDatabase, 
    exchangeClient exchange.ExchangeClient) (Portfolio, error) {

    if !general.ItemInSlice[datamodels.Asset](portfolioConfig.Assets, datamodels.USD) {
        return nil, errors.New("USD must be in the portfolio")
    }

    // verify no duplicate assets
    if !general.NoDuplicateItemsInSlice[datamodels.Asset](portfolioConfig.Assets) {
        return nil, errors.New("duplicate assets in portfolio")
    }

    // get feedConfigs from price aggregators
    assetFeedNames := map[datamodels.Asset]string{}
    for _, asset := range portfolioConfig.Assets {
        assetFeedNames[asset] = string(asset)
    }

    if portfolioConfig.DataDir == "" {
        return nil, errors.New("data_dir is required")
    }

    dataFeeds := map[string]feeds.DataFeed{}
    for _, asset := range portfolioConfig.Assets {
        if asset != datamodels.USD {
            priceFeedConfig, err := feeds.FillPartialFeedConfig(assetFeedNames[asset], 
                asset, 
                portfolioConfig.StartTime, 
                portfolioConfig.EndTime,
                portfolioConfig.DataDir,
            )
            if err != nil {
                return nil, err
            }
            dataFeed, err := feeds.NewDataFeedFromConfig(priceFeedConfig, db, exchangeClient)
            if err != nil {
                return nil, err
            }
            dataFeeds[string(asset)] = dataFeed
        }
    }

    priceAggregators := map[datamodels.Asset]aggregators.Aggregator{}
    for asset, assetFeedName := range assetFeedNames {
        if asset != datamodels.USD { 
        // USD should not need a price aggregator
            thisAggConfig := aggregators.FillPartialAggregatorConfig(assetFeedName + "_price", assetFeedName)
            feed := dataFeeds[assetFeedName]
            thisAggregator, err := aggregators.BuildAggregatorFromConfig(&thisAggConfig, db, feed)
            if err != nil {
                return nil, err
            }
            priceAggregators[asset] = thisAggregator
        } else {
            usdAggFunction := aggregators.CreateConstantValueAggregatorFunc(
                []string{},
                []string{},
                1.0,
            )
            usdPriceAggregator := aggregators.NewTimedAggregator(
                nil, 
                nil, 
                0, 
                100, 
                usdAggFunction,
                []string{},
                []string{},
            )
            priceAggregators[asset] = usdPriceAggregator
        }
    }

    dataAggregators := map[string]aggregators.Aggregator{}
    for _, aggregatorConfig := range portfolioConfig.Strategy.Aggregators {
        inputFeed := dataFeeds[aggregatorConfig.InputFeedName]
        thisAggregator, err := aggregators.BuildAggregatorFromConfig(&aggregatorConfig, db, inputFeed)
        if err != nil {
            return nil, err
        }
        dataAggregators[aggregatorConfig.Name] = thisAggregator
    }

    strategyEngine, err := strategies.StrategyEngineFromConfig(&portfolioConfig.Strategy, dataAggregators)
    if err != nil {
        return nil, err
    }

    portfolio, err := SimulatedPortfolioFromConfig(portfolioConfig, priceAggregators, strategyEngine)
    if err != nil {
        return nil, err
    }
    return portfolio, nil
}
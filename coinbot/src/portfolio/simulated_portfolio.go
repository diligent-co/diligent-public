package portfolio

import (
	"coinbot/src/aggregators"
	"coinbot/src/datamodels"
	"coinbot/src/metrics"
	"coinbot/src/strategies"
	"coinbot/src/utils/errors"
	"coinbot/src/utils/general"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
)

const chanWaitTime = time.Second
const portfolioMetricsUpdateInterval = time.Second * 1
const usdPriceUsd = 1.0

type PortfolioStrategySupplier struct {
	StrategyEngine       strategies.StrategyEngine
	StrategySubscription strategies.StrategySubscription
}

type PortfolioPriceSupplier struct {
	Aggregator   aggregators.Aggregator
	Subscription datamodels.DataPointSubscription
}

type PriceEntry struct {
	Asset     datamodels.Asset
	Price     float64
	Timestamp time.Time
}

type SimulatedPortfolio struct {
	aggregatorBuffer     map[string][]datamodels.DataPoint
	assetHasPrice        map[datamodels.Asset]bool
	assetHasPriceChannel chan datamodels.Asset
	assetPriceSuppliers  map[datamodels.Asset]PortfolioPriceSupplier
	coveredAssets        []datamodels.Asset
	ctx                  context.Context
	currentPrices        map[datamodels.Asset]PriceEntry
	done                 bool
	feedBuffer           map[string][]datamodels.DataPoint
	firstSignalArrived   bool
	highestValue         float64
	id                   string
	initialBalance       float64
	// lastPortfolioMetrics datamodels.PortfolioMetrics  // for feedback
	signalHistory        *general.TimedBuffer[*datamodels.Signal] // for feedback
	lowestValue          float64
	metrics              []datamodels.PortfolioRealTimeMetrics
	metricsSubscribers   []chan datamodels.Metric
	metricsWriter        metrics.MetricsWriter
	mutex                sync.RWMutex
	positions            map[datamodels.Asset]datamodels.Position
	strategyReadyChannel chan string
	strategyDoneChannel  chan string
	strategySuppliers    map[string]PortfolioStrategySupplier
	streamTimestamp      time.Time
	transactions         []datamodels.Transaction
}

func NewSimulatedPortfolio() *SimulatedPortfolio {
	nowBytes := []byte(time.Now().Format(time.RFC3339))
	id := general.GenerateUUID5StringFromByteArray(nowBytes)
	return &SimulatedPortfolio{
		aggregatorBuffer:     make(map[string][]datamodels.DataPoint),
		assetHasPrice:        make(map[datamodels.Asset]bool),
		assetHasPriceChannel: make(chan datamodels.Asset, 1),
		assetPriceSuppliers:  make(map[datamodels.Asset]PortfolioPriceSupplier),
		currentPrices:        make(map[datamodels.Asset]PriceEntry),
		done:                 false,
		feedBuffer:           make(map[string][]datamodels.DataPoint),
		firstSignalArrived:   false,
		id:                   id,
		// lastPortfolioMetrics: datamodels.PortfolioMetrics{},
		signalHistory:        general.NewTimedBuffer[*datamodels.Signal](100),
		metrics:              make([]datamodels.PortfolioRealTimeMetrics, 0),
		metricsSubscribers:   make([]chan datamodels.Metric, 0),
		metricsWriter:        nil,
		positions:            make(map[datamodels.Asset]datamodels.Position),
		strategyReadyChannel: make(chan string, 1),
		strategyDoneChannel:  make(chan string, 1),
		strategySuppliers:    make(map[string]PortfolioStrategySupplier),
		transactions:         make([]datamodels.Transaction, 0),
	}
}

func (p *SimulatedPortfolio) GetName() string {
	// use struct name without package
	name := strings.Split(reflect.TypeOf(p).String(), ".")[1]
	return name
}

func (p *SimulatedPortfolio) WithInitialBalance(balance float64) *SimulatedPortfolio {
	p.initialBalance = balance
	if p.positions == nil {
		p.positions = make(map[datamodels.Asset]datamodels.Position)
	}
	p.positions[datamodels.USD] = datamodels.Position{Asset: datamodels.USD, Amount: balance, AvgPrice: 1.0}
	return p
}

func (p *SimulatedPortfolio) WithStrategy(strategy strategies.StrategyEngine) *SimulatedPortfolio {
	p.strategySuppliers[strategy.GetName()] = PortfolioStrategySupplier{
		StrategyEngine: strategy,
	}
	return p
}

func (p *SimulatedPortfolio) WithStrategies(strategies []strategies.StrategyEngine) *SimulatedPortfolio {
	for _, strategy := range strategies {
		p.strategySuppliers[strategy.GetName()] = PortfolioStrategySupplier{
			StrategyEngine: strategy,
		}
	}
	return p
}

func (p *SimulatedPortfolio) WithAssets(assets []datamodels.Asset) *SimulatedPortfolio {
	p.coveredAssets = append(p.coveredAssets, assets...)
	if p.positions == nil {
		p.positions = make(map[datamodels.Asset]datamodels.Position)
	}
	for _, asset := range assets {
		if asset != datamodels.USD {
			p.positions[asset] = datamodels.Position{Asset: asset, Amount: 0, AvgPrice: 0}
		}
	}
	return p
}

func (p *SimulatedPortfolio) WithPriceAggregator(asset datamodels.Asset, priceAggregator aggregators.Aggregator) *SimulatedPortfolio {
	p.assetPriceSuppliers[asset] = PortfolioPriceSupplier{
		Aggregator: priceAggregator,
	}
	return p
}

func (p *SimulatedPortfolio) WithPriceAggregators(priceAggregators map[datamodels.Asset]aggregators.Aggregator) *SimulatedPortfolio {
	if priceAggregators == nil {
		slog.Error("Price aggregators map is nil, cannot set")
		return p
	}
	for asset, priceAggregator := range priceAggregators {
		p.assetPriceSuppliers[asset] = PortfolioPriceSupplier{
			Aggregator: priceAggregator,
		}
	}
	return p
}

func (p *SimulatedPortfolio) WithMetricsWriter(metricsWriter metrics.MetricsWriter) *SimulatedPortfolio {
	p.metricsWriter = metricsWriter
	return p
}

// func (p *SimulatedPortfolio) WithRewardFunction(rewardFunction strategies.RewardFunction) *SimulatedPortfolio {
// 	p.mutex.Lock()
// 	defer p.mutex.Unlock()
// 	p.rewardFunction = rewardFunction
// 	return p
// }

func (p *SimulatedPortfolio) getAssetHasPrice(asset datamodels.Asset) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	assetHasPriceFlag := p.assetHasPrice[asset]
	return assetHasPriceFlag
}

func (p *SimulatedPortfolio) setAssetHasPrice(asset datamodels.Asset, hasPrice bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.assetHasPrice[asset] = hasPrice
}

func (p *SimulatedPortfolio) getCashBalance() float64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	balance := p.positions["USD"].Amount
	return balance
}

func (p *SimulatedPortfolio) setCashBalance(cashBalance float64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.positions["USD"] = datamodels.Position{Asset: "USD", Amount: cashBalance, AvgPrice: 1.0}
}

func (p *SimulatedPortfolio) getPositions() map[datamodels.Asset]datamodels.Position {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	positionsCopy := make(map[datamodels.Asset]datamodels.Position)
	for asset, position := range p.positions {
		positionsCopy[asset] = position.Copy()
	}
	return positionsCopy
}

func (p *SimulatedPortfolio) setPositions(positions map[datamodels.Asset]datamodels.Position) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.positions = positions
}

func (p *SimulatedPortfolio) setPosition(asset datamodels.Asset, position datamodels.Position) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.positions[asset] = position
}

func (p *SimulatedPortfolio) setInitialState() {
	p.setHighestValue(p.initialBalance)
	p.setLowestValue(p.initialBalance)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.positions = make(map[datamodels.Asset]datamodels.Position)
	for _, asset := range p.coveredAssets {
		if asset == "USD" {
			p.positions[asset] = datamodels.Position{Asset: asset, Amount: p.initialBalance, AvgPrice: 1.0}
		} else {
			p.positions[asset] = datamodels.Position{Asset: asset, Amount: 0, AvgPrice: 0}
		}
	}

}

func (p *SimulatedPortfolio) getStrategySuppliers() map[string]PortfolioStrategySupplier {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.strategySuppliers
}

func (p *SimulatedPortfolio) setStrategySuppliers(strategySuppliers map[string]PortfolioStrategySupplier) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.strategySuppliers = strategySuppliers
}

func (p *SimulatedPortfolio) getPriceSuppliers() map[datamodels.Asset]PortfolioPriceSupplier {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.assetPriceSuppliers
}

func (p *SimulatedPortfolio) setPriceSuppliers(priceSuppliers map[datamodels.Asset]PortfolioPriceSupplier) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.assetPriceSuppliers = priceSuppliers
}

func (p *SimulatedPortfolio) setFirstSignalArrived(firstSignalArrived bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.firstSignalArrived = firstSignalArrived
}

func (p *SimulatedPortfolio) getFirstSignalArrived() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	newBool := p.firstSignalArrived
	return newBool
}

func (p *SimulatedPortfolio) updateLastStrategySignal(strategyName string, signal datamodels.Signal) {
	p.signalHistory.AddElement(&signal)
}

func (p *SimulatedPortfolio) getLastStrategySignal() (datamodels.Signal, bool) {
	signal, ok := p.signalHistory.GetLatestElement()
	if !ok {
		return datamodels.Signal{}, false
	}
	return *signal, true
}

// func (p *SimulatedPortfolio) setLastPortfolioMetrics(metrics datamodels.PortfolioMetrics) {
// 	p.mutex.Lock()
// 	defer p.mutex.Unlock()
// 	p.lastPortfolioMetrics = metrics
// }

// func (p *SimulatedPortfolio) getLastPortfolioMetrics() datamodels.PortfolioMetrics {
// 	p.mutex.RLock()
// 	defer p.mutex.RUnlock()
// 	return p.lastPortfolioMetrics
// }

func (p *SimulatedPortfolio) GetTransactions() []datamodels.Transaction {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	transactionsCopy := make([]datamodels.Transaction, len(p.transactions))
	for i, transaction := range p.transactions {
		transactionsCopy[i] = transaction.Copy()
	}
	return transactionsCopy
}

func (p *SimulatedPortfolio) addTransaction(transaction datamodels.Transaction) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.transactions = append(p.transactions, transaction)
}

func (p *SimulatedPortfolio) setTransactions(transactions []datamodels.Transaction) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.transactions = transactions
}

func (p *SimulatedPortfolio) getCoveredAssets() []datamodels.Asset {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	coveredAssetsCopy := make([]datamodels.Asset, len(p.coveredAssets))
	copy(coveredAssetsCopy, p.coveredAssets)
	return coveredAssetsCopy
}

func (p *SimulatedPortfolio) setHighestValue(highestValue float64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.highestValue = highestValue
}

func (p *SimulatedPortfolio) getHighestValue() float64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	highestValue := p.highestValue
	return highestValue
}

func (p *SimulatedPortfolio) setLowestValue(lowestValue float64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.lowestValue = lowestValue
}

func (p *SimulatedPortfolio) getLowestValue() float64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	lowestValue := p.lowestValue
	return lowestValue
}

func (p *SimulatedPortfolio) getDone() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	done := p.done
	return done
}

func (p *SimulatedPortfolio) setDone(done bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.done = done
}

func (p *SimulatedPortfolio) getStreamTimestamp() time.Time {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.streamTimestamp
}

func (p *SimulatedPortfolio) setStreamTimestamp(streamTimestamp time.Time) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.streamTimestamp = streamTimestamp
}

func (p *SimulatedPortfolio) getMetricMeasurements() []datamodels.PortfolioRealTimeMetrics {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	metricsCopy := make([]datamodels.PortfolioRealTimeMetrics, len(p.metrics))
	for i, metric := range p.metrics {
		metricsCopy[i] = metric.Copy()
	}
	return metricsCopy
}

func (p *SimulatedPortfolio) addMetricMeasurement(metric datamodels.PortfolioRealTimeMetrics) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics = append(p.metrics, metric)
}

func (p *SimulatedPortfolio) setCurrentPrice(priceEntry PriceEntry) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.currentPrices[priceEntry.Asset] = priceEntry
}

func (p *SimulatedPortfolio) getCurrentPrice(asset datamodels.Asset) (float64, error) {
	if asset == datamodels.USD {
		return usdPriceUsd, nil
	}
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	priceEntry, exists := p.currentPrices[asset]
	if !exists {
		slog.Error("Portfolio has no current price for asset", "asset", asset)
		return 0.0, fmt.Errorf("portfolio has no current price for asset: %s", asset)
	}
	return priceEntry.Price, nil
}

func (p *SimulatedPortfolio) SubscribeToMetrics() chan datamodels.Metric {
	ch := make(chan datamodels.Metric, 100)
	metrics := p.getMetricMeasurements()
	p.mutex.Lock()
	p.metricsSubscribers = append(p.metricsSubscribers, ch)
	if len(metrics) > 0 {
		for _, metric := range metrics {
			metricsBytes, err := json.Marshal(metric)
			if err != nil {
				slog.Error("Failed to marshal metrics", "error", err)
				continue
			}
			writableMetric := datamodels.Metric{
				MetricGeneratorId:   p.id,
				MetricGeneratorName: p.GetName(),
				MetricGeneratorType: datamodels.MetricGeneratorTypePortfolio,
				MetricTime:          metric.Timestamp,
				MetricName:          "portfolio_metrics",
				MetricValue:         metricsBytes,
			}
			select {
			case ch <- writableMetric:
				continue
			case <-time.After(chanWaitTime):
				slog.Warn("After waiting, portfolio metrics channel is still full, dropping metric")
			}
		}
	}
	p.mutex.Unlock()
	return ch
}

func (p *SimulatedPortfolio) getMetricsSubscribers() []chan datamodels.Metric {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.metricsSubscribers
}

func (p *SimulatedPortfolio) removeAllMetricsSubscribers() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metricsSubscribers = []chan datamodels.Metric{}
}

func (p *SimulatedPortfolio) Build() (*SimulatedPortfolio, error) {
	if len(p.coveredAssets) == 0 {
		slog.Error("Required assets list is nil or empty, cannot build portfolio")
		return p, errors.New("required assets list is nil or empty")
	}
	if !general.ItemInSlice(p.coveredAssets, datamodels.USD) {
		slog.Info("Adding USD to covered assets")
		p.coveredAssets = append(p.coveredAssets, datamodels.USD)
	}

	if len(p.assetPriceSuppliers) == 0 {
		slog.Error("Price aggregators map is nil or empty, cannot build portfolio")
		return p, errors.New("price aggregators map is nil or empty")
	}
	if len(p.positions) == 0 {	
		slog.Error("Positions map is nil or empty, cannot build portfolio")
		return p, errors.New("positions map is nil or empty")
	}
	if _, ok := p.positions[datamodels.USD]; !ok || p.positions[datamodels.USD].Amount == 0 {
		slog.Error("Initial USD Balance is 0, cannot build portfolio")
		return p, errors.New("initial USD balance is 0")
	}
	// TODO: get this working so we don't have to manually add the USD price aggregator every time
	if _, ok := p.assetPriceSuppliers[datamodels.USD]; !ok {
		slog.Info("Adding USD price aggregator")
		USDAggregator := aggregators.NewTimedAggregator(
			nil,
			nil,
			0,
			100*time.Millisecond,// TODO: make this configurable
			aggregators.CreateConstantValueAggregatorFunc([]string{"price"}, []string{"price"}, 1.0),
			[]string{"price"},
			[]string{"price"})
		// USDSubscription, err := USDAggregator.Subscribe(p.ctx, p.GetName())
		// if err != nil {
		// 	slog.Error("Failed to subscribe to USD price aggregator", "error", err)
		// 	return p, err
		// }
		p.assetPriceSuppliers[datamodels.USD] = PortfolioPriceSupplier{
			Aggregator: USDAggregator,
			// Subscription: *USDSubscription,
		}
	}



	// make sure every required asset has a price aggregator
	for _, asset := range p.coveredAssets {
		if _, ok := p.assetPriceSuppliers[asset]; !ok {
			slog.Error("Price aggregator for asset not found", "asset", asset)
			return p, errors.New("price aggregator for asset not found")
		}
		p.currentPrices[asset] = PriceEntry{Price: 0.0, Timestamp: time.Time{}, Asset: asset}
		p.assetHasPrice[asset] = false
	}
	// check that there are signal subscriptions
	if len(p.strategySuppliers) == 0 {
		slog.Error("No strategies, cannot build portfolio")
		return p, errors.New("no strategies")
	}
	return p, nil
}

func (p *SimulatedPortfolio) Start(ctx context.Context) error {
	if p.IsStarted() {
		slog.Warn("Portfolio already started, skipping")
		return nil
	}
	p.ctx = ctx
	p.setInitialState()
	p.setDone(false)
	for asset, priceSupplier := range p.assetPriceSuppliers {
		priceAggregatorSubscription, err := priceSupplier.Aggregator.Subscribe(p.ctx, p.GetName())
		if err != nil {
			slog.Error("Failed to subscribe to price aggregator", "error", err)
			return err
		}
		// set subscription for supplier, update struct-level listing
		p.mutex.Lock()
		priceSupplier.Subscription = *priceAggregatorSubscription
		p.assetPriceSuppliers[asset] = priceSupplier
		p.mutex.Unlock()
		// handle subscription
		if !priceSupplier.Aggregator.IsStarted() {
			slog.Info(fmt.Sprintf("Portfolio's price aggregator not started, starting price supplier for asset %s", asset),
				"aggregator", priceSupplier.Aggregator.GetName())
			priceAggregatorStartErr := priceSupplier.Aggregator.Start(p.ctx)
			if priceAggregatorStartErr != nil {
				slog.Error("Failed to start price aggregator", "error", priceAggregatorStartErr)
				return priceAggregatorStartErr
			}
		}
		go p.processPriceAggregatorDataPoints(asset, priceAggregatorSubscription)
	}

	p.waitForAssetPrices()
	for name, strategySupplier := range p.strategySuppliers {
		slog.Info("Portfolio subscribing to strategy", "portfolio", p.GetName(), "strategy", strategySupplier.StrategyEngine.GetName())
		strategySubscriptionName := p.GetName() + "_" + strategySupplier.StrategyEngine.GetName()
		strategySubscription, err := strategySupplier.StrategyEngine.Subscribe(p.ctx, strategySubscriptionName)
		if err != nil {
			slog.Error("Failed to subscribe to strategy", "error", err)
			return err
		}
		p.mutex.Lock()
		strategySupplier.StrategySubscription = strategySubscription
		p.strategySuppliers[name] = strategySupplier
		p.mutex.Unlock()
		slog.Info("Portfolio successfully subscribed to strategy", "portfolio", p.GetName(), "strategy", strategySupplier.StrategyEngine.GetName())
		
		if !strategySupplier.StrategyEngine.IsStarted() {
			slog.Info(fmt.Sprintf("%s's strategy not started, starting %s", p.GetName(), strategySupplier.StrategyEngine.GetName()))
			strategyStartErr := strategySupplier.StrategyEngine.Start(p.ctx)
			if strategyStartErr != nil {
				slog.Error("Failed to start strategy", "error", strategyStartErr)
				return strategyStartErr
			}
		}
		
		go p.processStrategySignals(strategySubscription)
	}

	p.funnelStrategyReadyChannels()

	p.waitForStrategies()

	go p.stopWhenStrategiesDone()
	return nil
}

func (p *SimulatedPortfolio) IsStarted() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	// TODO:make this more robust
	return p.ctx != nil
}

func (p *SimulatedPortfolio) processPriceAggregatorDataPoints(
	asset datamodels.Asset,
	priceAggregatorSubscription *datamodels.DataPointSubscription) {
	for {
		if p.getDone() {
			slog.Info("Portfolio price aggregator data point processing stopped due to portfolio being done")
			return
		}

		select {
		case <-p.ctx.Done():
			slog.Info("Portfolio price aggregator data point processing stopped due to context being done", "aggregator", priceAggregatorSubscription.SubscriptionName)
			return
		case err := <-priceAggregatorSubscription.ErrorChan:
			slog.Error("Error in price aggregator data point processing", "error", err)
			return
		case <-priceAggregatorSubscription.DoneChan:
			slog.Info("Portfolio price aggregator data point processing stopped due to price aggregator being done", "aggregator", priceAggregatorSubscription.SubscriptionName)
			return
		case priceDataPoint, ok := <-priceAggregatorSubscription.DataPointChan:
			if !ok {
				slog.Info("Portfolio price aggregator data point channel closed", "aggregator", priceAggregatorSubscription.SubscriptionName)
				return
			}

			currentLoad := len(priceAggregatorSubscription.DataPointChan)
			capacity := cap(priceAggregatorSubscription.DataPointChan)
			loadProportion := float64(currentLoad) / float64(capacity)
			if loadProportion >= 0.9 {
				slog.Warn(fmt.Sprintf("Portfolio price aggregator data point channel load warning: %d/%d in %s", currentLoad, capacity, priceAggregatorSubscription.SubscriptionName))
			}
			// update current price
			if !ok {
				slog.Info("Portfolio price aggregator data point channel closed")
				return
			}
			p.updateCurrentPrice(asset, priceDataPoint)
			// check if this price aggregator has data
			if !p.getAssetHasPrice(asset) {
				p.setAssetHasPrice(asset, true)
				p.assetHasPriceChannel <- asset
			}
		}
	}
}

// takes ReadyChan from each strategy and creates a single channel that sends the strategy names
func (p *SimulatedPortfolio) funnelStrategyReadyChannels() {
	for _, strategySupplier := range p.strategySuppliers {
		go func() {
			<-strategySupplier.StrategySubscription.ReadyChan
			p.strategyReadyChannel <- strategySupplier.StrategySubscription.SubscriptionName
		}()
	}
}

func (p *SimulatedPortfolio) waitForAssetPrices() {
	slog.Info("Waiting for asset prices", "portfolio", p.GetName())
	assetHasPriceMap := make(map[datamodels.Asset]bool)
	for _, asset := range p.coveredAssets {
		assetHasPriceMap[asset] = false
	}
	assetsRemaining := len(p.coveredAssets)

	for assetsRemaining > 0 {
		select {
		case <-p.ctx.Done():
			slog.Info("Portfolio asset price waiting stopped due to context being done")
			return
		case asset, ok := <-p.assetHasPriceChannel:
			if !ok {
				slog.Error("Portfolio asset has price channel closed", "portfolio", p.GetName())
				return
			}
			assetHasPriceMap[asset] = true
			assetsRemaining--
			remainingAssetsString := ""
			for asset, hasPrice := range assetHasPriceMap {
				if !hasPrice {
					remainingAssetsString += fmt.Sprintf("%s,", asset)
				}
			}
			slog.Info("Portfolio asset price received",
				"asset", asset,
				"assetsRemaining", assetsRemaining,
				"remainingAssets", remainingAssetsString)
		case <-time.After(1 * time.Minute):
			slog.Info("Portfolio asset price waiting timed out", "portfolio", p.GetName())
			return
		}
	}
	slog.Info("All asset prices received", "portfolio", p.GetName())
}

func (p *SimulatedPortfolio) waitForStrategies() {
	slog.Info("Waiting for strategie ready-signals", "portfolio", p.GetName())
	strategiesRemaining := len(p.strategySuppliers)

	for strategiesRemaining > 0 {
		select {
		case <-p.ctx.Done():
			slog.Info("Portfolio strategy ready signal waiting stopped due to context being done")
			return
		case strategyName, ok := <-p.strategyReadyChannel:
			if !ok {
				slog.Error("Portfolio strategy ready channel closed", "portfolio", p.GetName())
				return
			}
			slog.Info("Portfolio strategy ready signal received", "portfolio", p.GetName(), "strategy", strategyName)
			strategiesRemaining--
		case <-time.After(1 * time.Minute):
			slog.Info("Portfolio strategy ready signal waiting timed out", "portfolio", p.GetName())
			return

		}
	}
	slog.Info("All strategies ready", "portfolio", p.GetName())
}

func (p *SimulatedPortfolio) stopWhenStrategiesDone() {
	strategySuppliers := p.getStrategySuppliers()
	strategiesDone := 0
	for strategiesDone < len(strategySuppliers) {
		strategyName, ok := <-p.strategyDoneChannel
		if !ok {
			slog.Error("Portfolio strategy done channel closed", "portfolio", p.GetName())
			return
		}
		strategiesDone++
		slog.Info(fmt.Sprintf("Portfolio marked strategy %d/%d done", strategiesDone, len(strategySuppliers)), "portfolio", p.GetName(), "strategy", strategyName)
	}
	slog.Info("All strategies done, stopping portfolio", "portfolio", p.GetName())
	p.broadcastDone()
	p.setDone(true)
	p.ctx.Done()
}

func (p *SimulatedPortfolio) processStrategySignals(strategySubscription strategies.StrategySubscription) {
	slog.Info("Portfolio waiting for strategy signals",
		"portfolio", p.GetName(),
		"strategy", strategySubscription.SubscriptionName)

	portfolioMetricsTimer := time.NewTimer(portfolioMetricsUpdateInterval)
	defer portfolioMetricsTimer.Stop()

	for {
		if p.getDone() {
			slog.Info("Portfolio strategy signal processing stopped due to portfolio being done")
			return
		}

		select {
		case <-p.ctx.Done():
			slog.Info("Portfolio strategy signal processing stopped due to context being done")
			return
		case err, ok := <-strategySubscription.ErrorChan:
			if !ok {
				slog.Info("Portfolio strategy error channel closed")
				return
			}
			slog.Error("Error in strategy signal processing", "error", err)
			return
		case _, ok := <-strategySubscription.DoneChan:
			if !ok {
				slog.Info("Portfolio strategy done channel closed")
				return
			}
			slog.Info("Portfolio marked strategy done", "portfolio", p.GetName(), "strategy", strategySubscription.SubscriptionName)
			p.strategyDoneChannel <- strategySubscription.SubscriptionName
		case signal, ok := <-strategySubscription.SignalChan:
			if !ok {
				slog.Info("Portfolio strategy signal channel closed")
				return
			}
			// capacity warning
			if general.ChannelAtLoadLevel[datamodels.Signal](strategySubscription.SignalChan, 0.8) {
				slog.Warn("Portfolio strategy incoming signal channel load warning for subscriber", "subscriber", strategySubscription.SubscriptionName)
			}

			// Log received signal
			slog.Debug("Portfolio received strategy signal",
				"portfolio", p.GetName(),
				"strategy", strategySubscription.SubscriptionName,
				"signal", signal)

			if !p.getFirstSignalArrived() {
				slog.Info("Portfolio received first signal")
				// handle first signal
				signalTimestamp := signal.StreamTime
				p.setStreamTimestamp(signalTimestamp)
				// mark starting portfolio metrics with signal timestamp
				p.setFirstSignalArrived(true)
				p.updateLastStrategySignal(strategySubscription.SubscriptionName, signal)
				go p.updatePortfolioMetrics()
			} else {
				slog.Info("Portfolio received a signal")
				// handle all subsequent signals
				// calculate and emit reward for last signal
				p.calculateAndEmitRewardForLastSignal(strategySubscription)
				p.updateLastStrategySignal(strategySubscription.SubscriptionName, signal)
				signalTimestamp := signal.StreamTime
				p.setStreamTimestamp(signalTimestamp)
			}

			// Default to "default" asset if not specified
			switch signal.SignalType {
			case datamodels.SignalTypeAllocation:
				allocations := make(map[datamodels.Asset]float64)
				for asset, allocation := range signal.Allocations {
					allocations[asset] = allocation
				}
				orders, err := p.CalculateReallocationOrders(allocations)
				if err != nil {
					slog.Error("Portfolio failed to calculate reallocation orders", "error", err)
					continue
				}
				_ = orders
				for _, order := range orders {
					err := p.ExecuteOrder(order)
					if err != nil {
						slog.Error("Portfolio failed to execute reallocation order", "error", err)
					}
				}
				continue
			default:
				slog.Error("Portfolio received unknown signal type", "signal", signal)
				continue
			}
		case <-portfolioMetricsTimer.C:
			slog.Info("Portfolio updating metrics", "portfolio", p.GetName())
			go p.updatePortfolioMetrics()
			portfolioMetricsTimer.Reset(portfolioMetricsUpdateInterval)
		}
	}
}

func (p *SimulatedPortfolio) Stop() error {

	// Mark done to signal processing loops to stop
	//p.setDone(true)

	// Close all metric subscriber channels
	slog.Info("Portfolio closing metrics subscriber channels")
	p.removeAllMetricsSubscribers()

	slog.Info("Portfolio unsubscribing from strategies")
	strategySuppliers := p.getStrategySuppliers()
	// Unsubscribe from all strategies
	for _, strategySupplier := range strategySuppliers {
		subId := strategySupplier.StrategySubscription.SubscriptionId
		unsubscribeErr := strategySupplier.StrategyEngine.Unsubscribe(p.ctx, subId)
		if unsubscribeErr != nil {
			slog.Error("Failed to unsubscribe from strategy", "error", unsubscribeErr)
		}
	}
	p.setStrategySuppliers(nil)

	priceSuppliers := p.getPriceSuppliers()

	slog.Info("Portfolio unsubscribing from price aggregators")
	// Unsubscribe from all price aggregators
	for _, priceSupplier := range priceSuppliers {
		subId := priceSupplier.Subscription.SubscriptionId
		unsubscribeErr := priceSupplier.Aggregator.Unsubscribe(p.ctx, subId)
		if unsubscribeErr != nil {
			slog.Error("Failed to unsubscribe from price aggregator", "error", unsubscribeErr)
		}
	}
	p.setPriceSuppliers(nil)

	return nil
}

// CalculateReallocationOrders calculates the orders needed to achieve target allocations
// allocations is a map of asset -> target percentage (0.0 to 1.0)
func (p *SimulatedPortfolio) CalculateReallocationOrders(targetAllocations map[datamodels.Asset]float64) ([]Order, error) {
	thesePrices := make(map[datamodels.Asset]float64)
	thesePositions := make(map[datamodels.Asset]datamodels.Position)
	cashBalance := p.getCashBalance()
	coveredAssets := p.getCoveredAssets()

	totalValue := cashBalance

	for _, asset := range coveredAssets {
		currentPrice, err := p.getCurrentPrice(asset)
		if err != nil {
			return nil, fmt.Errorf("portfolio is missing or zero price for asset: %s", asset)
		}
		thesePrices[asset] = currentPrice
		position, err := p.GetPosition(asset)
		if err != nil {
			return nil, fmt.Errorf("portfolio has no position for asset: %s", asset)
		}
		thesePositions[asset] = position
		totalValue = position.Amount * thesePrices[asset]
	}

	// Validate allocations
	var totalAllocation float64
	for _, allocation := range targetAllocations {
		if allocation < 0 || allocation > 1 {
			return nil, fmt.Errorf("portfolio has invalid allocation: must be between 0 and 1")
		}
		totalAllocation += allocation
	}
	if totalAllocation < 0.99 || totalAllocation > 1.01 {
		return nil, fmt.Errorf("portfolio allocations must sum to 1.0 (got %.2f)", totalAllocation)
	}

	var sellOrders []Order
	var buyOrders []Order

	// First pass: Calculate all needed changes and separate into buys and sells
	for asset, targetAllocation := range targetAllocations {
		if asset == datamodels.USD {
			continue
		}
		targetValue := totalValue * targetAllocation
		currentPrice, err := p.getCurrentPrice(asset)
		if err != nil {
			return nil, fmt.Errorf("portfolio is missing or zero price for asset: %s", asset)
		}

		currentPosition := thesePositions[asset]
		currentValue := currentPosition.Amount * currentPrice
		valueDiff := targetValue - currentValue

		if math.Abs(valueDiff) < 0.000001 {
			continue // Skip if change is negligible
		}

		amount := math.Abs(valueDiff / currentPrice)
		if valueDiff < 0 {
			// This is a sell order
			sellOrders = append(sellOrders, Order{
				Asset:  asset,
				Side:   datamodels.OrderSideSell,
				Amount: amount,
			})
		} else {
			// This is a buy order
			buyOrders = append(buyOrders, Order{
				Asset:  asset,
				Side:   datamodels.OrderSideBuy,
				Amount: amount,
			})
		}
	}

	// Calculate total funds needed for buys and available from sells
	var totalBuyFunds float64
	for _, order := range buyOrders {
		price := thesePrices[order.Asset]
		totalBuyFunds += order.Amount * price * 1.005 // Include 0.5% fee
	}

	var totalSellFunds float64
	for _, order := range sellOrders {
		price := thesePrices[order.Asset]
		totalSellFunds += order.Amount * price * 0.995 // Account for 0.5% fee
	}

	// Check if we need to scale down buy orders due to insufficient funds
	availableFunds := cashBalance + totalSellFunds
	if totalBuyFunds > availableFunds {
		// Scale down all buy orders proportionally
		scaleFactor := availableFunds / totalBuyFunds
		for i := range buyOrders {
			buyOrders[i].Amount *= scaleFactor
		}
	}

	// Combine orders with sells first, then buys
	return append(sellOrders, buyOrders...), nil
}

// Order represents a buy or sell order
type Order struct {
	Asset  datamodels.Asset
	Side   datamodels.OrderSide
	Amount float64
}

func (p *SimulatedPortfolio) ExecuteOrder(order Order) error {
	if order.Amount <= 0 {
		return fmt.Errorf("portfolio has invalid order parameters")
	}
	if order.Asset == "" {
		return fmt.Errorf("portfolio has invalid order parameters")
	}
	side := order.Side
	amount := order.Amount
	asset := order.Asset

	currentAssetPrice, err := p.getCurrentPrice(asset)
	if err != nil {
		return fmt.Errorf("portfolio has no current price for asset: %s", asset)
	}
	currentCashBalance := p.getCashBalance()
	currentAssetPosition, err := p.GetPosition(asset)
	newAssetPosition := currentAssetPosition
	if err != nil {
		return fmt.Errorf("portfolio has no position for asset: %s", asset)
	}
	totalTransactionValue := amount * currentAssetPrice
	// Add simple fee calculation
	fees := totalTransactionValue * 0.001 // 0.1% fee

	if side == datamodels.OrderSideBuy {
		if totalTransactionValue+fees > currentCashBalance {
			return fmt.Errorf("portfolio has insufficient funds")
		}

		// Update position
		newAssetTotalValue := (currentAssetPosition.Amount * currentAssetPosition.AvgPrice) + totalTransactionValue // not sure of the value of this
		newAssetTotalAmount := currentAssetPosition.Amount + amount
		newAssetPosition.AvgPrice = newAssetTotalValue / newAssetTotalAmount
		newAssetPosition.Amount = newAssetTotalAmount

		// Update cash balance
		p.setCashBalance(currentCashBalance - (totalTransactionValue + fees))
		p.setPosition(asset, newAssetPosition)
	} else if side == datamodels.OrderSideSell {
		if currentAssetPosition.Amount < amount {
			return fmt.Errorf("portfolio has insufficient asset balance")
		}
		newAssetPosition.Amount -= amount

		// Update cash balance
		p.setCashBalance(currentCashBalance + (totalTransactionValue - fees))
		p.setPosition(asset, newAssetPosition)
	} else {
		return fmt.Errorf("portfolio has unknown order side: %s", side)
	}

	// Record transaction
	p.addTransaction(datamodels.Transaction{
		Timestamp: time.Now(),
		Asset:     asset,
		Side:      side,
		Amount:    amount,
		Price:     currentAssetPrice,
		Total:     totalTransactionValue,
		Fees:      fees,
	})

	return nil
}

func (p *SimulatedPortfolio) GetPosition(asset datamodels.Asset) (datamodels.Position, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	position, exists := p.positions[asset]
	if !exists {
		return datamodels.Position{}, fmt.Errorf("portfolio has no position for asset: %s", asset)
	}
	return position, nil
}

func (p *SimulatedPortfolio) GetAllocations() map[datamodels.Asset]float64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	allocations := make(map[datamodels.Asset]float64)
	for asset, position := range p.positions {
		totalValue := p.GetTotalValue()
		if totalValue <= 0 {
			allocations[asset] = 0
		} else {
			allocations[asset] = (position.Amount * position.AvgPrice) / totalValue
		}
	}
	return allocations
}

// Implement performance tracking methods
func (p *SimulatedPortfolio) calculatePortfolioGrowthPct(initialValue float64, currentValue float64) float64 {
	if initialValue == 0 {
		return 0
	}
	return (currentValue - initialValue) / initialValue
}

func (p *SimulatedPortfolio) calculateDrawdown(highestValue float64, currentValue float64) float64 {
	if highestValue == 0 {
		return 0
	}
	if currentValue == 0 {
		return 0
	}
	if currentValue > highestValue {
		return 0
	}
	return (highestValue - currentValue) / highestValue
}

func (p *SimulatedPortfolio) GetTotalValue() float64 {
	// positions includes cashBalance as a USD position
	totalValue := 0.0
	positions := p.getPositions()

	// Add value of all positions
	for asset, position := range positions {
		if position.Amount > 0 {
			currentPrice, err := p.getCurrentPrice(asset)
			if err != nil {
				slog.Error("Portfolio has no current price for asset", "asset", asset)
				continue
			}
			totalValue += position.Amount * currentPrice
		}
	}

	return totalValue
}

// GetSharpeRatio calculates the Sharpe ratio using historical metrics
// Returns 0 if insufficient data points (need at least 2 for standard deviation)
func GetSharpeRatio(riskFreeRate float64, transactions []datamodels.Transaction) float64 {
	// need to find the return of every sell transaction, based on the price difference with the last buy transaction

	return 0
}

// GetSortinoRatio calculates the Sortino ratio using historical metrics
// Similar to Sharpe but only considers downside volatility
// Returns 0 if insufficient data points
func GetSortinoRatio(riskFreeRate float64, transactions []datamodels.Transaction) float64 {

	return 0
}

func (p *SimulatedPortfolio) updatePortfolioMetrics() {

	metrics := datamodels.PortfolioRealTimeMetrics{}

	// read operations
	currentTotalValue := p.GetTotalValue()
	currentHighestValue := p.getHighestValue()
	if currentTotalValue > currentHighestValue {
		p.setHighestValue(currentTotalValue)
		currentHighestValue = currentTotalValue
	}
	currentLowestValue := p.getLowestValue()
	if currentTotalValue < currentLowestValue {
		p.setLowestValue(currentTotalValue)
		currentLowestValue = currentTotalValue
	}
	streamTime := p.getStreamTimestamp()
	positions := p.getPositions()
	allocations := p.GetAllocations()
	cashBalance := p.getCashBalance()

	// write operations

	metrics.Timestamp = streamTime
	metrics.TotalValue = currentTotalValue
	metrics.CashBalance = cashBalance
	metrics.PortfolioGrowthPct = p.calculatePortfolioGrowthPct(p.initialBalance, currentTotalValue)
	metrics.Drawdown = p.calculateDrawdown(currentHighestValue, currentTotalValue)
	metrics.Positions = positions
	metrics.Allocations = allocations

	p.addMetricMeasurement(metrics)
	metricsBytes, err := json.Marshal(metrics)
	if err != nil {
		slog.Error("Portfolio has error marshalling json from metrics", "error", err, "metrics", metrics)
		return
	}

	writableMetric := datamodels.Metric{
		MetricGeneratorId:   p.id,
		MetricGeneratorName: p.GetName(),
		MetricGeneratorType: datamodels.MetricGeneratorTypePortfolio,
		MetricTime:          metrics.Timestamp,
		MetricName:          "portfolio_metrics",
		MetricValue:         metricsBytes,
	}

	if p.metricsWriter != nil {
		p.metricsWriter.Write(context.Background(), writableMetric)
	}

	// Notify subscribers
	if !p.getDone() {
		p.broadcastWritableMetric(writableMetric)
	}
}

func (p *SimulatedPortfolio) calculateAndEmitRewardForLastSignal(strategySubscription strategies.StrategySubscription) {
	lastSignal, ok := p.getLastStrategySignal()
	if !ok {
		slog.Warn("Portfolio is trying to calculate reward but has no last signal")
		return
	}
	lastSignalReward := p.GetTotalValue() - p.initialBalance

	rewardSignal := datamodels.RewardRecord{
		WriteTime:  time.Now(),
		SignalId:   lastSignal.SignalId,
		StreamTime: lastSignal.StreamTime,
		Reward:     lastSignalReward,
	}

	// broadcast reward signal back to strategies
	processRewards := func(signal datamodels.RewardRecord) {
		select {
		case strategySubscription.FeedbackChan <- signal:
			if general.ChannelAtLoadLevel[datamodels.RewardRecord](strategySubscription.FeedbackChan, 0.8) {
				slog.Warn("Portfolio outgoing reward channel load warning for subscriber", "subscriber", strategySubscription.SubscriptionName)
			}
			slog.Debug("Portfolio sent reward feedback to strategy", "strategy", strategySubscription.SubscriptionName)
			return
		case <-time.After(chanWaitTime):
			slog.Error("Timeout waiting for subscriber '%s' to receive reward signal", "subscriber", strategySubscription.SubscriptionName)
			return
		}
	}
	processRewards(rewardSignal)
}

func (p *SimulatedPortfolio) updateCurrentPrice(
	// needs a write lock it writes to currentPrices
	asset datamodels.Asset,
	dp datamodels.DataPoint) error {

	if dp == nil {
		slog.Error("Price data point is nil for aggregator", "aggregator", p.assetPriceSuppliers[asset].Aggregator.GetName())
		return fmt.Errorf("price data point is nil")
	}

	price, err := dp.Get("price")
	if err != nil {
		slog.Error("Price data point does not contain price", "data_point", dp)
		return fmt.Errorf("price data point does not contain price")
	}
	priceFloat, err := price.GetTypedValue()
	if err != nil {
		slog.Error("Price data point does not contain price", "data_point", dp)
		return fmt.Errorf("price data point does not contain price")
	}
	_ = priceFloat
	p.setCurrentPrice(PriceEntry{Price: priceFloat.(float64), Timestamp: dp.GetTimestamp(), Asset: asset})
	return nil
}

func (p *SimulatedPortfolio) broadcastDone() {
	for _, ch := range p.getMetricsSubscribers() {
		close(ch)
	}
}

func (p *SimulatedPortfolio) broadcastWritableMetric(metric datamodels.Metric) {
	for _, ch := range p.getMetricsSubscribers() {
		if ch == nil {
			slog.Error("Portfolio has nil metrics subscriber")
			continue
		}
		select {
		case ch <- metric:
		case <-time.After(chanWaitTime):
			slog.Error("Timeout waiting for subscriber '%s' to receive metrics", "subscriber", ch)
		}
	}
}

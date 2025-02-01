package strategies

import "coinbot/src/datamodels"

type RewardFunction func(metrics *datamodels.PortfolioRealTimeMetrics) float64

func ReturnsRewardFunction(metrics *datamodels.PortfolioRealTimeMetrics) float64 {
	return metrics.PortfolioGrowthPct
}

func TotalValueRewardFunction(metrics *datamodels.PortfolioRealTimeMetrics) float64 {
	return metrics.TotalValue
}

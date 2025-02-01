package datamodels

import (
	"time"
)

type Asset string

const (
	XBT Asset = "XBT"
	USD Asset = "USD"
	ETH Asset = "ETH"
	SOL Asset = "SOL"
	// more coins
	ADA Asset = "ADA"
)

type Transaction struct {
	Timestamp time.Time
	Asset     Asset
	Side      OrderSide
	Amount    float64
	Price     float64
	Total     float64
	Fees      float64
}

func (t *Transaction) Copy() Transaction {
	return Transaction{
		Timestamp: t.Timestamp,
		Asset:     t.Asset,
		Side:      t.Side,
		Amount:    t.Amount,
		Price:     t.Price,
		Total:     t.Total,
		Fees:      t.Fees,
	}
}

type Position struct {
	Asset    Asset   `json:"asset"`
	Amount   float64 `json:"amount,omitempty"`
	AvgPrice float64 `json:"avg_price,omitempty"`
}

func (p *Position) Copy() Position {
	return Position{
		Asset:    p.Asset,
		Amount:   p.Amount,
		AvgPrice: p.AvgPrice,
	}
}

type PortfolioRealTimeMetrics struct {
	TriggeringSignalId        string             `json:"triggering_signal_id,omitempty"`
	TriggeringSignalTimestamp time.Time          `json:"triggering_signal_timestamp"`
	Timestamp                 time.Time          `json:"timestamp"`
	TotalValue                float64            `json:"total_value,omitempty"`
	CashBalance               float64            `json:"cash_balance,omitempty"`
	PortfolioGrowthPct        float64            `json:"portfolio_growth_pct,omitempty"`
	Drawdown                  float64            `json:"drawdown,omitempty"`
	Positions                 map[Asset]Position `json:"positions,omitempty"`
	Allocations               map[Asset]float64  `json:"allocations,omitempty"`
}

func (pm *PortfolioRealTimeMetrics) GetId() string {
	return pm.TriggeringSignalId
}

func (pm *PortfolioRealTimeMetrics) GetTimestamp() time.Time {
	return pm.Timestamp
}

func (pm *PortfolioRealTimeMetrics) Copy() PortfolioRealTimeMetrics {
	return PortfolioRealTimeMetrics{
		TriggeringSignalId:        pm.TriggeringSignalId,
		TriggeringSignalTimestamp: pm.TriggeringSignalTimestamp,
		Timestamp:                 pm.Timestamp,
		TotalValue:                pm.TotalValue,
		CashBalance:               pm.CashBalance,
		PortfolioGrowthPct:        pm.PortfolioGrowthPct,
		Drawdown:                  pm.Drawdown,
		Positions:                 pm.Positions,
		Allocations:               pm.Allocations,
	}
}

type MetricGeneratorType string

const (
	MetricGeneratorTypeFeed       MetricGeneratorType = "feed"
	MetricGeneratorTypeAggregator MetricGeneratorType = "aggregator"
	MetricGeneratorTypeStrategy   MetricGeneratorType = "strategy"
	MetricGeneratorTypePortfolio  MetricGeneratorType = "portfolio"
)

// metric can be scalar, vector, or matrix
type Metric struct {
	BaseModel
	MetricGeneratorId   string              `gorm:"not null;index"`
	MetricGeneratorName string              `gorm:"not null;index"`
	MetricGeneratorType MetricGeneratorType `gorm:"not null;index"`
	MetricTime          time.Time           `gorm:"not null;index"`
	MetricName          string              `gorm:"not null;index"`
	MetricValue         []byte              `gorm:"not null;type:json"`
}

type MetricGenerator struct {
	BaseModel
	MetricGeneratorName string              `gorm:"not null;index"`
	MetricGeneratorType MetricGeneratorType `gorm:"not null;index"`
}

package datamodels

import (
	"time"

	"github.com/google/uuid"
)

type BaseModel struct {
	Id        int64 `gorm:"primarykey"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type BaseModelUUID struct {
	ID        uuid.UUID `gorm:"primarykey;default:gen_random_uuid();type:uuid"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type OrderType string

const (
	/*
		Possible values: [limit, market, iceberg, stop-loss, stop-loss-limit, take-profit, take-profit-limit, trailing-stop, trailing-stop-limit, settle-position]
	*/
	OrderTypeMarket            OrderType = "market"
	OrderTypeLimit             OrderType = "limit"
	OrderTypeIceberg           OrderType = "iceberg"
	OrderTypeStopLoss          OrderType = "stop-loss"
	OrderTypeStopLossLimit     OrderType = "stop-loss-limit"
	OrderTypeTakeProfit        OrderType = "take-profit"
	OrderTypeTakeProfitLimit   OrderType = "take-profit-limit"
	OrderTypeTrailingStop      OrderType = "trailing-stop"
	OrderTypeTrailingStopLimit OrderType = "trailing-stop-limit"
	OrderTypeSettlePosition    OrderType = "settle-position"
)

type OrderSide string

const (
	OrderSideBuy  OrderSide = "buy"
	OrderSideSell OrderSide = "sell"
)

type CoinbotTrade struct {
	BaseModel
	Symbol    string    `gorm:"not null;index"`
	OrderSide OrderSide `gorm:"not null"`
	Price     float64   `gorm:"not null"`
	Quantity  float64   `gorm:"not null"`
	OrderType OrderType `gorm:"not null;index"`
	Timestamp time.Time `gorm:"not null;index"`
}

type CoinbotSnapshotTimeIncrement int

const (
	CoinbotSnapshotTimeIncrement60Minutes CoinbotSnapshotTimeIncrement = 60
)

type CoinbotSnapshot struct {
	BaseModel
	Symbol         string                       `gorm:"not null"`
	Increment      CoinbotSnapshotTimeIncrement `gorm:"not null"`
	StartTimestamp time.Time                    `gorm:"not null"`
	EndTimestamp   time.Time                    `gorm:"not null"`
	High           float64                      `gorm:"not null"`
	Low            float64                      `gorm:"not null"`
	Open           float64                      `gorm:"not null"`
	Close          float64                      `gorm:"not null"`
	VolumeUsd      float64                      `gorm:"not null"`
	VolumeCoin     float64                      `gorm:"not null"`
}

type TransactionSet interface {
	GetTransactionCount() int
}

type KrakenTransactionSet struct {
	Transactions []KrakenTransactionHistory
}

func (t *KrakenTransactionSet) GetTransactionCount() int {
	return len(t.Transactions)
}

type KrakenTransactionHistory struct {
	BaseModel
	Timestamp     time.Time `gorm:"not null;index"`
	Symbol1       string    `gorm:"not null;index"` // index
	Symbol2       string    `gorm:"not null;index"` // index
	Symbol1Amount float64   `gorm:"not null"`
	Symbol2Amount float64   `gorm:"not null"`
}

package exchange

import (
	"context"

	"coinbot/src/datamodels"
)

type ExchangeClient interface {
	GetName() string
	GetOutputFieldNames(subscriptionName string) []string
	IsConnected() bool
	Start(ctx context.Context) error
	AddFeed(ctx context.Context, channelNames datamodels.KrakenDataChannel, pairs string) error
	GetServerTime(ctx context.Context) (int64, error)
	GetSystemStatus(ctx context.Context) (string, error)
	Subscribe(ctx context.Context, subscriberName string, channelName datamodels.KrakenDataChannel, pair string) (*datamodels.DataPointSubscription, error)
	Unsubscribe(ctx context.Context, subscriptionId string) error
	GetTrades(ctx context.Context, pairV2 string, since int64, count int) ([]datamodels.DataPoint, int64, error)
	AddOrder(ctx context.Context, orderParams datamodels.KrakenV2OrderParams) error
	CancelOrder(ctx context.Context, orderParams datamodels.KrakenV2CancelOrderParams) error
}
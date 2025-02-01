package feeds

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"coinbot/src/datamodels"
	"coinbot/src/exchange"
)

// TestDataPoint implements DataPoint interface for testing
type TestDataPoint struct {
	timestamp time.Time
	values    map[string]float64
}

func NewTestDataPoint() *TestDataPoint {
	return &TestDataPoint{
		timestamp: time.Now(),
		values:    make(map[string]float64),
	}
}

func (d *TestDataPoint) GetTimestamp() time.Time                     { return d.timestamp }
func (d *TestDataPoint) GetFieldValue(field string) (float64, error) { return d.values[field], nil }
func (d *TestDataPoint) GetFields() []string {
	fields := make([]string, 0, len(d.values))
	for k := range d.values {
		fields = append(fields, k)
	}
	return fields
}
func (d *TestDataPoint) SetFieldValue(field string, value float64) error {
	d.values[field] = value
	return nil
}

func (d *TestDataPoint) GetFieldType(field string) string {
	return "float64"
}

type MockExchangeClient struct {
	exchange.ExchangeClient
}

func NewMockExchangeClient() *MockExchangeClient {
	return &MockExchangeClient{}
}

func (m *MockExchangeClient) Start(ctx context.Context) error {
	return nil
}

func (m *MockExchangeClient) AddFeed(ctx context.Context, channelNames datamodels.KrakenDataChannel, pairs string) error {
	return nil
}

func (m *MockExchangeClient) GetServerTime(ctx context.Context) (int64, error) {
	return 0, nil
}

func (m *MockExchangeClient) GetSystemStatus(ctx context.Context) (string, error) {
	return "", nil
}

func (m *MockExchangeClient) Subscribe(ctx context.Context, subscriberName string, channelName datamodels.KrakenDataChannel, pairs string) (*datamodels.DataPointSubscription, error) {
	dataChan := make(chan datamodels.DataPoint, 100)
	done := make(chan struct{})
	errors := make(chan error)

	return &datamodels.DataPointSubscription{
		SubscriptionName: fmt.Sprintf("%s_%s_%s", subscriberName, channelName, pairs),
		SubscriptionId:   uuid.New().String(),
		DataPointChan:    dataChan,
		DoneChan:         done,
		ErrorChan:        errors,
	}, nil
}

func (m *MockExchangeClient) Unsubscribe(ctx context.Context, subscriptionId string) error {
	return nil
}

func (m *MockExchangeClient) GetTrades(ctx context.Context, pairV2 string, since int64, count int) ([]datamodels.DataPoint, int64, error) {
	return nil, 0, nil
}

func (m *MockExchangeClient) AddOrder(ctx context.Context, orderParams datamodels.KrakenV2OrderParams) error {
	return nil
}

func (m *MockExchangeClient) CancelOrder(ctx context.Context, orderParams datamodels.KrakenV2CancelOrderParams) error {
	return nil
}

func (m *MockExchangeClient) GetDataBetween(ctx context.Context, pair string, since int64, count int) ([]datamodels.DataPoint, int64, error) {
	return nil, 0, nil
}

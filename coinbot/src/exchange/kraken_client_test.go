package exchange

import (
	"coinbot/src/datamodels"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

// Change from const to var
var (
	restUrl   = "https://api.kraken.com"
	wsUrlPub  = "wss://ws.kraken.com"
	wsUrlPriv = "wss://ws-auth.kraken.com"
)

func TestConvertMessageFromBytes(t *testing.T) {
	messageString := `[119930892,{"a":["67421.90000",2,"2.84160469"],"b":["67421.60000",3,"3.27384588"],"c":["67421.60000","0.43416460"],"v":["1228.91284399","1472.55849966"],"p":["68405.47150","68486.93527"],"t":[24912,29417],"l":["67213.00000","67213.00000"],"h":["69430.60000","69430.60000"],"o":["68748.70000","68847.10000"]},"ticker","XBT/USD"]`
	messageBytes := []byte(messageString)

	data, err := krakenV1MessageFromBytes(messageBytes)
	if err != nil {
		t.Errorf("Error converting message: %v", err)
	}

	t.Logf("Data: %v", data)
}

func TestUnmarshalRequestAckV2(t *testing.T) {
	messageString := `{"error":"","method":"subscribe","success":true,"time_in":"1725494400.000","time_out":"1725494460.000"}`
	messageBytes := []byte(messageString)

	var incomingRequestAck datamodels.KrakenRequestAckV2
	err := unmarshalRequestAck(messageBytes, &incomingRequestAck)
	if err != nil {
		t.Errorf("Error unmarshalling request ack: %v", err)
	}

	messageString2 := `{"error":"EGeneral:Unknown error","method":"subscribe","success":false,"time_in":"1725494400.000","time_out":"1725494460.000"}`
	messageBytes2 := []byte(messageString2)

	var incomingRequestAck2 datamodels.KrakenRequestAckV2
	err = unmarshalRequestAck(messageBytes2, &incomingRequestAck2)
	if err != nil {
		t.Errorf("Error unmarshalling request ack: %v", err)
	}

}

func TestUnmarshalTickerSnapshotDataV1(t *testing.T) {
	messageString := `{"a":["69480.20000",12,"12.38898066"],"b":["69480.10000",0,"0.00227973"],"c":["69480.10000","0.00055826"],"v":["1826.71516776","2114.77837537"],"p":["69404.24072","69117.95499"],"t":[26670,32318],"l":["67481.40000","66836.00000"],"h":["70482.00000","70482.00000"],"o":["67787.80000","67334.90000"]}`
	messageBytes := []byte(messageString)

	var data interface{}
	err := json.Unmarshal(messageBytes, &data)
	assert.NoError(t, err)

	result, err := krakenTickerSnapshotDataFromInterface(data)
	assert.NoError(t, err)
	t.Logf("Data: %v", result)
}

func TestKrakenV1MessageFromBytes(t *testing.T) {
	rawMessageTicker := `[119930892,{"a":["67421.90000",2,"2.84160469"],"b":["67421.60000",3,"3.27384588"],"c":["67421.60000","0.43416460"],"v":["1228.91284399","1472.55849966"],"p":["68405.47150","68486.93527"],"t":[24912,29417],"l":["67213.00000","67213.00000"],"h":["69430.60000","69430.60000"],"o":["68748.70000","68847.10000"]},"ticker","XBT/USD"]`
	messageBytesTicker := []byte(rawMessageTicker)

	var err error

	dataTicker, err := krakenV1MessageFromBytes(messageBytesTicker)
	assert.NoError(t, err)

	// send data field to appropriate handler
	assert.Equal(t, datamodels.KrakenDataChannelTicker, dataTicker.ChannelName)
	_, err = krakenTickerSnapshotDataFromInterface(dataTicker.Data)
	assert.NoError(t, err)

	rawMessageTrade := `[0,[["200","200", "300", "s", "l", ""],["201","201", "301", "s", "l", ""]],"trade","XBT/USD"]`
	messageBytesTrade := []byte(rawMessageTrade)

	dataTrade, err := krakenV1MessageFromBytes(messageBytesTrade)
	assert.NoError(t, err)

	assert.Equal(t, datamodels.KrakenDataChannelTrade, dataTrade.ChannelName)
	_, err = krakenV1TradesFromTradesResultInterface(dataTrade.Data)
	assert.NoError(t, err)

}

var upgrader = websocket.Upgrader{}

func TestNewKrakenClient(t *testing.T) {
	cfg := datamodels.KrakenConfig{
		APIKey:    "test-key",
		APISecret: "test-secret",
		WsVersion: 2,
	}

	client := NewKrakenClient(cfg)
	assert.NotNil(t, client)
	assert.Equal(t, "test-key", client.apiKey)
	assert.Equal(t, "test-secret", client.apiSecret)
	assert.Equal(t, 2, client.wsVersion)
}

func TestGetApiSignature(t *testing.T) {
	testCases := []struct {
		name          string
		urlPath       string
		data          map[string]interface{}
		secret        string
		shouldSucceed bool
	}{
		{
			name:          "valid signature generation",
			urlPath:       "/0/private/GetWebSocketsToken",
			data:          map[string]interface{}{},
			secret:        "dGVzdC1zZWNyZXQ=", // base64 encoded "test-secret"
			shouldSucceed: true,
		},
		{
			name:          "invalid secret",
			urlPath:       "/0/private/GetWebSocketsToken",
			data:          map[string]interface{}{},
			secret:        "invalid-secret",
			shouldSucceed: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			signature, encodedData, err := GetApiSignature(tc.urlPath, tc.data, tc.secret)
			if tc.shouldSucceed {
				assert.NoError(t, err)
				assert.NotEmpty(t, signature)
				assert.NotEmpty(t, encodedData)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestSubscriptionNameHandling(t *testing.T) {
	testCases := []struct {
		name           string
		subscriberName string
		channelName    datamodels.KrakenDataChannel
		pair           string
		expected       string
	}{
		{
			name:           "valid subscription name",
			subscriberName: "test",
			channelName:    datamodels.KrakenDataChannelTicker,
			pair:           "XBT/USD",
			expected:       "test_ticker_XBT/USD",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := createOutboundSubscriptionName(tc.subscriberName, tc.channelName, tc.pair)
			assert.Equal(t, tc.expected, result)

			// Test separation
			subName, channel, pair := separateOutboundSubscriptionName(result)
			assert.Equal(t, tc.subscriberName, subName)
			assert.Equal(t, tc.channelName, channel)
			assert.Equal(t, tc.pair, pair)
		})
	}
}

func TestDataPointConversion(t *testing.T) {
	rawDataString := `[{"symbol":"BTC/USD","bid":80721.0,"bid_qty":5.83922754,"ask":80721.1,"ask_qty":0.94249250,"last":80721.0,"volume":4997.67795459,"vwap":79408.3,"low":76555.0,"high":81480.7,"change":4149.7,"change_pct":5.42}]`
	var parsedData []interface{}
	err := json.Unmarshal([]byte(rawDataString), &parsedData)
	assert.NoError(t, err)

	datapoints, err := dataToDataPoint(parsedData)
	assert.NoError(t, err)
	assert.NotNil(t, datapoints)

	genericDP, ok := datapoints[0].(*datamodels.GenericDataPoint)
	assert.True(t, ok)

	symbol, err := genericDP.GetFieldValue("symbol")
	assert.NoError(t, err)
	assert.Equal(t, "BTC/USD", symbol.(string))

	bid, err := genericDP.GetFieldValue("bid")
	assert.NoError(t, err)
	assert.Equal(t, 80721.0, bid.(float64))

	bidQty, err := genericDP.GetFieldValue("bid_qty")
	assert.NoError(t, err)
	assert.Equal(t, 5.83922754, bidQty.(float64))
}

// Mock WebSocket server for testing WebSocket connections
func setupMockWebSocket(t *testing.T) (*httptest.Server, chan string) {
	messages := make(chan string, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		assert.NoError(t, err)
		defer conn.Close()

		for msg := range messages {
			err = conn.WriteMessage(websocket.TextMessage, []byte(msg))
			assert.NoError(t, err)
		}
	}))

	return server, messages
}

func TestWebSocketConnection(t *testing.T) {
	server, messages := setupMockWebSocket(t)
	defer server.Close()
	defer close(messages)

	// Override WebSocket URLs for testing
	originalPublicUrl := wsUrlPub
	originalPrivateUrl := wsUrlPriv
	wsUrlPub = "ws" + server.URL[4:]
	wsUrlPriv = "ws" + server.URL[4:]
	defer func() {
		wsUrlPub = originalPublicUrl
		wsUrlPriv = originalPrivateUrl
	}()

	client := NewKrakenClient(datamodels.KrakenConfig{
		APIKey:    "test-key",
		APISecret: "dGVzdC1zZWNyZXQ=",
		WsVersion: 2,
	})

	// Set a valid token to avoid the actual API call
	client.webSocketSignature = "test-token"
	client.webSocketTokenExpiresAt = time.Now().Add(time.Hour)

	err := client.Connect(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, client.publicConn)
	assert.NotNil(t, client.privateConn)

	client.Close()
}

func TestSeparateOutboundSubscriptionName(t *testing.T) {
	tests := []struct {
		name               string
		subscriptionName   string
		wantFeedStructName string
		wantDataType       datamodels.KrakenDataChannel
		wantPair          string
	}{
		{
			name:               "simple subscription name",
			subscriptionName:   "MyFeed_trade_BTC/USD",
			wantFeedStructName: "MyFeed",
			wantDataType:       datamodels.KrakenDataChannel("trade"),
			wantPair:          "BTC/USD",
		},
		{
			name:               "subscription name with underscores in feed name",
			subscriptionName:   "My_Custom_Feed_trade_ETH/USD",
			wantFeedStructName: "My",
			wantDataType:       datamodels.KrakenDataChannel("trade"),
			wantPair:          "ETH/USD",
		},
		{
			name:               "subscription with ticker channel",
			subscriptionName:   "PriceFeed_ticker_XRP/USD",
			wantFeedStructName: "PriceFeed",
			wantDataType:       datamodels.KrakenDataChannel("ticker"),
			wantPair:          "XRP/USD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFeedStructName, gotDataType, gotPair := separateOutboundSubscriptionName(tt.subscriptionName)

			if gotFeedStructName != tt.wantFeedStructName {
				t.Errorf("separateOutboundSubscriptionName() feedStructName = %v, want %v", 
					gotFeedStructName, tt.wantFeedStructName)
			}
			if gotDataType != tt.wantDataType {
				t.Errorf("separateOutboundSubscriptionName() dataType = %v, want %v", 
					gotDataType, tt.wantDataType)
			}
			if gotPair != tt.wantPair {
				t.Errorf("separateOutboundSubscriptionName() pair = %v, want %v", 
					gotPair, tt.wantPair)
			}
		})
	}
}
package exchange

import (
	"coinbot/src/datamodels"
	"coinbot/src/utils/errors"
	"coinbot/src/utils/general"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const RestUrl = "https://api.kraken.com/0"
const WebSocketUrlV1 = "wss://ws.kraken.com"
const WebSocketUrlV2Public = "wss://ws.kraken.com/v2"
const WebSocketUrlV2Private = "wss://ws-auth.kraken.com/v2"

type Handler func([]interface{})

type KrakenClient struct {
	config                  datamodels.KrakenConfig
	publicConn              *websocket.Conn
	privateConn             *websocket.Conn
	wsVersion               int
	inboundChannels         []string // channelName_pair
	subscribers             map[string]*datamodels.DataPointSubscription
	mu                      sync.RWMutex
	apiKey                  string
	apiSecret               string
	webSocketSignature      string
	webSocketTokenExpiresAt time.Time
	started                 bool
}

func NewKrakenClient(config datamodels.KrakenConfig) *KrakenClient {
	apiKey := config.APIKey
	apiSecret := config.APISecret
	version := config.WsVersion

	return &KrakenClient{
		wsVersion:       version,
		config:          config,
		apiKey:          apiKey,
		apiSecret:       apiSecret,
		subscribers:     make(map[string]*datamodels.DataPointSubscription),
		inboundChannels: make([]string, 0),
		started:         false,
	}
}

func (k *KrakenClient) GetName() string {
	thisStructName := strings.Split(reflect.TypeOf(k).String(), ".")[1]
	return thisStructName + "_WSv" + strconv.Itoa(k.wsVersion)
}

func (k *KrakenClient) GetOutputFieldNames(subscriptionName string) []string {
	var results []string
	_, channelName, _ := separateOutboundSubscriptionName(subscriptionName)
	switch channelName {
	case datamodels.KrakenDataChannelTicker:
		/* API response:
		"symbol": "ALGO/USD",
		"bid": 0.10025,
		"bid_qty": 740.0,
		"ask": 0.10036,
		"ask_qty": 1361.44813783,
		"last": 0.10035,
		"volume": 997038.98383185,
		"vwap": 0.10148,
		"low": 0.09979,
		"high": 0.10285,
		"change": -0.00017,
		"change_pct": -0.17*/
		results = []string{"symbol",
			"bid", "bid_qty", "ask", "ask_qty",
			"last", "volume", "vwap", "low",
			"high", "change", "change_pct"}
	case datamodels.KrakenDataChannelTrade:
		/* API response:
		"symbol": "MATIC/USD",
		"side": "buy",
		"price": 0.5147,
		"qty": 6423.46326,
		"ord_type": "limit",
		"trade_id": 4665846,
		"timestamp": "2023-09-25T07:48:36.925533Z"
		*/
		results = []string{"symbol",
			"side",
			"price",
			"qty",
			"ord_type",
			"trade_id",
			"timestamp"}
	case datamodels.KrakenDataChannelBook:
		results = []string{"symbol",
			"bids",
			"asks",
			"checksum"}
	default:
		panic(fmt.Sprintf("unknown channel name: %s", channelName))
	}

	return results
}

func (k *KrakenClient) IsConnected() bool {
	return k.publicConn != nil && k.privateConn != nil
}

func (k *KrakenClient) Connect(ctx context.Context) error {
	if k.IsConnected() {
		return nil
	}
	slog.Info(fmt.Sprintf("%s connecting to websocket v%d", k.GetName(), k.wsVersion))
	if k.wsVersion == 1 {
		return k.connectWebSocketV1(ctx)
	} else if k.wsVersion == 2 {
		return k.connectWebSocketV2(ctx)
	}
	return nil
}

func (k *KrakenClient) connectWebSocketV1(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.Dial(WebSocketUrlV1, nil)
	if err != nil {
		return fmt.Errorf("dial error: %v", err)
	}
	k.publicConn = conn
	k.wsVersion = 1
	return nil
}

func (k *KrakenClient) connectWebSocketV2(ctx context.Context) error {
	if k.isWebSocketTokenExpiredOrInvalid() {
		if err := k.refreshWebSocketToken(); err != nil {
			return fmt.Errorf("error getting web socket token: %v", err)
		}
	}
	publicDialer := websocket.Dialer{
		ReadBufferSize:  32 * 1024,
		WriteBufferSize: 32 * 1024,
	}

	// set headers
	headers := http.Header{}
	// headers.Set("API-Key", k.apiKey)
	// headers.Set("Authorization", "Bearer "+k.webSocketSignature)
	publicConn, _, err := publicDialer.Dial(WebSocketUrlV2Public, headers)
	if err != nil {
		slog.Error(fmt.Sprintf("%s failed to connect to public websocket: %v", k.GetName(), err))
		return fmt.Errorf("%s failed to connect to public websocket: %v", k.GetName(), err)
	}
	k.publicConn = publicConn

	privateDialer := websocket.Dialer{
		ReadBufferSize:  32 * 1024,
		WriteBufferSize: 32 * 1024,
	}

	privateConn, _, err := privateDialer.Dial(WebSocketUrlV2Private, headers)
	if err != nil {
		slog.Error(fmt.Sprintf("%s failed to connect to private websocket: %v", k.GetName(), err))
		return fmt.Errorf("%s failed to connect to private websocket: %v", k.GetName(), err)
	}
	k.privateConn = privateConn

	slog.Info(fmt.Sprintf("%s connected to public and private websocket", k.GetName()))
	return nil
}

func (k *KrakenClient) Start(ctx context.Context) error {
	// connect to websocket
	if !k.IsConnected() {
		if err := k.Connect(ctx); err != nil {
			return err
		}
	}

	// Start public message handler
	go func() {
		for {
			select {
			case <-ctx.Done():
				slog.Info(fmt.Sprintf("%s exiting public message handler", k.GetName()))
				return
			default:
				messageType, message, err := k.publicConn.ReadMessage()
				if err != nil {
					if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
						slog.Info(fmt.Sprintf("%s public websocket closed normally", k.GetName()))
						return
					} else {
						slog.Error(fmt.Sprintf("%s public websocket read error: %v", k.GetName(), err))
						slog.Error("Attempting to reconnect")
						k.Close()
						if err := k.Connect(ctx); err != nil {
							slog.Error(fmt.Sprintf("%s failed to reconnect to websocket: %v", k.GetName(), err))
							time.Sleep(time.Second * 5)
							continue
						}

						// re-subscribe to all channels
						for _, channel := range k.inboundChannels {
							channelParts := strings.Split(channel, "_")
							channelName := channelParts[0]
							pair := channelParts[1]
							if err := k.AddFeed(ctx, datamodels.KrakenDataChannel(channelName), pair); err != nil {
								slog.Error(fmt.Sprintf("%s failed to re-subscribe to channel %s: %v", k.GetName(), channel, err))
							}
						}
						continue
					}
				}
				if messageType == websocket.PingMessage {
					k.publicConn.WriteMessage(websocket.PongMessage, nil)
					continue
				}
				go k.handleMessages(message)
			}
		}
	}()

	// Start private message handler if using v2
	if k.wsVersion == 2 && k.privateConn != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					slog.Info(fmt.Sprintf("%s exiting private message handler", k.GetName()))
					return
				default:
					messageType, message, err := k.privateConn.ReadMessage()
					if err != nil {
						if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
							slog.Info(fmt.Sprintf("%s private websocket closed normally", k.GetName()))
						} else {
							slog.Error(fmt.Sprintf("%s private websocket read error: %v", k.GetName(), err))
						}
						return
					}
					if messageType == websocket.PingMessage {
						k.privateConn.WriteMessage(websocket.PongMessage, nil)
						continue
					}
					go k.handleMessages(message)
				}
			}
		}()
	}

	k.started = true
	return nil
}

func (k *KrakenClient) GetServerTime(ctx context.Context) (int64, error) {
	url := fmt.Sprintf("%s/public/Time", RestUrl)
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("error getting server time: %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	var response datamodels.KrakenRestResponse[datamodels.KrakenServerTimeResult]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, err
	}

	return response.Result.UnixTime, nil
}

func (k *KrakenClient) GetSystemStatus(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/public/SystemStatus", RestUrl)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var response datamodels.KrakenRestResponse[datamodels.KrakenSystemStatusResult]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", err
	}

	return response.Result.Status, nil
}

func GetApiSignature(urlPath string, data map[string]interface{}, secret string) (string, string, error) {
	nonce := strconv.FormatInt(time.Now().UnixMilli(), 10)
	data["nonce"] = nonce

	urlValues := url.Values{}
	for key, value := range data {
		urlValues.Set(key, fmt.Sprintf("%v", value))
	}
	// convert data into json string
	urlEncodedData := urlValues.Encode()

	// url-encode the json string
	stringToEncode := nonce + urlEncodedData

	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(stringToEncode))
	sha256HashBytes := sha256Hash.Sum(nil)

	encodedSecret, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", "", errors.Wrap(err, "error decoding api secret")
	}
	h := hmac.New(sha512.New, encodedSecret)
	message := append([]byte(urlPath), sha256HashBytes...)
	h.Write(message)
	hmacDigest := h.Sum(nil)
	apiSignature := base64.StdEncoding.EncodeToString(hmacDigest)
	return apiSignature, urlEncodedData, nil
}

func (k *KrakenClient) AddFeed(ctx context.Context, channelName datamodels.KrakenDataChannel, pair string) error {
	if k.publicConn == nil {
		return fmt.Errorf("no connection to websocket")
	}

	// check if we already have the channel in inboundChannels
	newFeedName := createInboundSubscriptionName(channelName, pair)
	if k.IsSubscribed(channelName, pair) {
		return nil
	}

	subscribeErrs := make([]error, 0)
	slog.Info(fmt.Sprintf("Adding %s to %s", newFeedName, k.GetName()))
	var msg interface{}
	if k.wsVersion == 1 {
		msg = datamodels.KrakenSubscribeRequestV1{
			Event: "subscribe",
			Pair:  []string{pair},
			Subscription: struct {
				Name  string  `json:"name"`
				Token *string `json:"token"`
			}{
				Name: string(channelName),
			},
		}

		subscribeErr := k.publicConn.WriteJSON(msg)
		if subscribeErr != nil {
			subscribeErrs = append(subscribeErrs, subscribeErr)
		}
	} else if k.wsVersion == 2 {
		// only ownTrades needs a token
		if !isPrivateConnectionSubscription(channelName) {
			msg = datamodels.KrakenRequestV2{
				Method: "subscribe",
				Params: struct {
					Channel string   `json:"channel"`
					Symbol  []string `json:"symbol"`
				}{
					Channel: string(channelName),
					Symbol:  []string{pair},
				},
			}
			subscribeErr := k.publicConn.WriteJSON(msg)
			if subscribeErr != nil {
				subscribeErrs = append(subscribeErrs, subscribeErr)
			}

		} else {
			if k.isWebSocketTokenExpiredOrInvalid() {
				if err := k.refreshWebSocketToken(); err != nil {
					return err
				}
			}

			msg := datamodels.KrakenRequestV2{
				Method: "subscribe",
				Params: struct {
					Channel string  `json:"channel"`
					Token   *string `json:"token"`
				}{
					Channel: string(channelName),
					Token:   &k.webSocketSignature,
				},
			}
			subscribeErr := k.privateConn.WriteJSON(msg)
			if subscribeErr != nil {
				subscribeErrs = append(subscribeErrs, subscribeErr)
			}
		}
	}

	if len(subscribeErrs) > 0 {
		return errors.Newf("error subscribing to feeds: %v", subscribeErrs)
	}

	k.inboundChannels = append(k.inboundChannels, newFeedName)
	return nil
}

func (k *KrakenClient) Subscribe(ctx context.Context,
	subscriberName string,
	krakenDataType datamodels.KrakenDataChannel,
	pair string) (*datamodels.DataPointSubscription, error) {

	if !k.IsConnected() {
		slog.Warn("Kraken client is not connected, attempting to connect")
		if err := k.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// subscribe successful, now we need to build a subscription
	subscriptionName := createOutboundSubscriptionName(subscriberName, krakenDataType, pair)
	subscriptionId := uuid.New().String()
	dataChan := make(chan datamodels.DataPoint, 100)
	done := make(chan struct{})
	errors := make(chan error)

	subscription := &datamodels.DataPointSubscription{
		SubscriptionName: subscriptionName,
		SubscriptionId:   subscriptionId,
		DataPointChan:    dataChan,
		DoneChan:         done,
		ErrorChan:        errors,
	}

	// add feeds for this subscription, in case they're not already coming in
	subscriptionErr := k.AddFeed(ctx, krakenDataType, pair)
	if subscriptionErr != nil {
		return nil, subscriptionErr
	}

	k.subscribers[subscriptionId] = subscription
	return subscription, nil
}

func (k *KrakenClient) Unsubscribe(ctx context.Context, subscriptionId string) error {
	// remove from subscribers
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.subscribers, subscriptionId)
	return nil
}

func (k *KrakenClient) GetTrades(ctx context.Context, pairV2 string, since int64, count int) ([]datamodels.DataPoint, int64, error) {

	pairV1 := krakenPairV2ToPairV1(pairV2)

	// since cannot be in the future
	if since > int64(time.Now().Unix()) {
		return nil, 0, errors.New("since cannot be in the future")
	}

	// count needs to be between 1 and 1000
	if count < 1 || count > 1000 {
		return nil, 0, errors.New("count must be between 1 and 1000")
	}

	url := fmt.Sprintf("%s/public/Trades?pair=%s&since=%d&count=%d", RestUrl, pairV1, since, count)
	resp, err := http.Get(url)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error getting order book")
	}
	defer resp.Body.Close()

	// decode into KrakenRestGetTradesResult
	var responseStruct datamodels.KrakenRestResponse[interface{}]
	err = json.NewDecoder(resp.Body).Decode(&responseStruct)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error decoding trades")
	}

	if len(responseStruct.Error) > 0 {
		return nil, 0, errors.Newf("error in response: %v", responseStruct.Error)
	}

	/*
		response.Result loks like {"PAIRNAME": [[trade1], [trade2], ...], "last": 1234567890}
		we want to return the trades for the pair, and the last field
	*/
	tradesResult, ok := responseStruct.Result.(map[string]interface{})
	if !ok {
		return nil, 0, errors.New("error with type inference for trades result")
	}

	var timestampOfLastTrade string
	var trades []datamodels.KrakenV1Trade
	var tradesConvErr error

	// iterate over keys
	for key, value := range tradesResult {
		if key == "last" {
			timestampOfLastTrade = value.(string)
		} else {
			trades, tradesConvErr = krakenV1TradesFromTradesResultInterface(value)
			if tradesConvErr != nil {
				return nil, 0, errors.Wrap(tradesConvErr, "error extracting trades from results array")
			}
		}
	}

	timestampOfLastTradeInt, err := strconv.ParseInt(timestampOfLastTrade, 10, 64)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error converting timestamp of last trade to int")
	}

	tradesInterface := make([]interface{}, len(trades))
	for i, trade := range trades {
		tradesInterface[i] = trade
	}

	tradeDatapoints, err := dataToDataPoint(tradesInterface)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error converting trades to data points")
	}

	return tradeDatapoints, timestampOfLastTradeInt, nil
}

func (k *KrakenClient) refreshWebSocketToken() error {
	// post request to GetWebSocketsToken
	urlPath := "/0/private/GetWebSocketsToken"
	apiSignature, urlEncodedData, err := GetApiSignature(urlPath, map[string]interface{}{}, k.apiSecret)
	if err != nil {
		return err
	}
	// turn post data into a reader
	apiPostDataReader := strings.NewReader(urlEncodedData)

	client := &http.Client{}
	req, err := http.NewRequest("POST", RestUrl+"/private/GetWebSocketsToken", apiPostDataReader)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return err
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("API-Key", k.apiKey)
	req.Header.Add("API-Sign", apiSignature)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	res, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending request: %v", err)
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
		return err
	}

	var response datamodels.KrakenRestResponse[datamodels.KrakenWebSocketsTokenResult]
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("Error unmarshalling response: %v", err)
		log.Printf("Response: %s", string(body))
		return err
	}
	if len(response.Error) > 0 {
		log.Printf("Error in response: %v", response.Error)
		return fmt.Errorf("error in response: %v", response.Error)
	}

	k.webSocketSignature = response.Result.Token
	tokenTtlSeconds := response.Result.Expires
	k.webSocketTokenExpiresAt = time.Now().Add(time.Duration(tokenTtlSeconds) * time.Second)
	if k.webSocketSignature == "" {
		log.Printf("No web socket token received in response")
		log.Printf("Response: %s", string(body))
		return fmt.Errorf("no web socket token received in response")
	}
	return nil
}

func (k *KrakenClient) isWebSocketTokenExpiredOrInvalid() bool {
	if k.webSocketSignature == "" {
		return true
	}
	if time.Now().After(k.webSocketTokenExpiresAt.Add(-10 * time.Second)) {
		return true
	}
	return false
}

func (k *KrakenClient) AddOrder(ctx context.Context, orderParams datamodels.KrakenV2OrderParams) error {
	return k.addOrderWs(orderParams)
}

func (k *KrakenClient) addOrderWs(orderParams datamodels.KrakenV2OrderParams) error {
	if k.privateConn == nil {
		return fmt.Errorf("no connection to websocket")
	}

	msg := datamodels.KrakenRequestV2{
		Method: "add_order",
		Params: orderParams,
	}

	return k.privateConn.WriteJSON(msg)
}

func (k *KrakenClient) CancelOrder(ctx context.Context, orderParams datamodels.KrakenV2CancelOrderParams) error {
	return k.cancelOrderWs(orderParams)
}

func (k *KrakenClient) cancelOrderWs(orderParams datamodels.KrakenV2CancelOrderParams) error {
	if k.privateConn == nil {
		return fmt.Errorf("no connection to websocket")
	}

	msg := datamodels.KrakenRequestV2{
		Method: "cancel_order",
		Params: orderParams,
	}

	return k.privateConn.WriteJSON(msg)
}

func (k *KrakenClient) handleMessages(rawMessage []byte) {
	slog.Debug("Received ", "message", string(rawMessage))
	if k.wsVersion == 1 {
		k.handleMessageV1(rawMessage)
		return
	} else if k.wsVersion == 2 {
		k.handleMessageV2(rawMessage)
	}
}

func (k *KrakenClient) handleMessageV1(rawMessage []byte) {
	// First try to unmarshal as an event message
	log.Printf("Received message: %s", string(rawMessage))

	// attempt to read as heartbeat message
	var message datamodels.KrakenMessageV1
	err := json.Unmarshal(rawMessage, &message)
	if err == nil {
		log.Printf("Heartbeat: %v", message)
		return
	}

	if message.Event == "heartbeat" {
		return
	} else if message.Event == "subscriptionStatus" {
		log.Printf("SubscriptionStatus: %v", message)
		return
	} else if message.Event == "systemStatus" {
		log.Printf("SystemStatus: %v", message)
		return
	} else {
		// read as incoming tuple and convert it
		message, err = krakenV1MessageFromBytes(rawMessage)
		if err != nil {
			log.Printf("Error converting message: %v", err)
			return
		}
	}

	// based on the incomging message's channel name, pair, and event,
	// find the subscription(s) that need to be updated
	// this is based on the subscription name, which is a combination of the subscriber name, channel name, and pair
	// so we'll be matching off the channel name and pair
	datapoints, err := dataToDataPoint([]interface{}{message.Data})
	if err != nil {
		slog.Warn("Error converting message to data point", "error", err, "message", string(rawMessage))
		return
	}
	thisMessageChannelName := message.ChannelName
	thisMessagePair := message.Pair
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, subscription := range k.subscribers {
		_, outboundChannelName, outboundPair := separateOutboundSubscriptionName(subscription.SubscriptionName)
		if outboundChannelName == thisMessageChannelName && outboundPair == thisMessagePair {
			for _, datapoint := range datapoints {
				subscription.DataPointChan <- datapoint
			}
		}
	}
}

func (k *KrakenClient) handleMessageV2(rawMessage []byte) {
	var incomingRequestAck datamodels.KrakenRequestAckV2
	err := unmarshalRequestAck(rawMessage, &incomingRequestAck)
	if err == nil {
		log.Print(incomingRequestAck.ToString())
		return
	}

	var incomingMessage datamodels.KrakenIncomingMessageV2
	err = unmarshalIncomingMessage(rawMessage, &incomingMessage)
	if err != nil {
		log.Printf("Error unmarshalling incoming message as KrakenIncomingMessageV2: %v, Message: %s", err, string(rawMessage))
		return
	}
	slog.Debug("Incoming v2 message, unmarshalled", "message", incomingMessage)

	if incomingMessage.Channel == datamodels.KrakenDataChannelStatus {
		// read Data as json
		var data []map[string]interface{}
		var err error
		if incomingMessage.Data != nil {
			data, err = getDataAsJsonArray(*incomingMessage.Data)
			if err != nil {
				slog.Warn("Error getting data as json", "error", err, "message", string(rawMessage))
				return
			}
		}
		slog.Debug("Status message", "data", data)
		return
	}

	if incomingMessage.Channel == datamodels.KrakenDataChannelHeartbeat {
		slog.Debug("Heartbeat message", "message", incomingMessage)
		return
	}

	slog.Debug("Message is not a status or heartbeat message", "message", incomingMessage)

	// for other channel types, we need to send it on
	thisMessageChannelName := incomingMessage.Channel
	var thisMessagePair string
	if incomingMessage.Data != nil {
		dataJsonArray, err := getDataAsJsonArray(*incomingMessage.Data)
		if err != nil {
			log.Printf("Error getting data as json: %v", err)
			return
		}
		for _, dataJson := range dataJsonArray {
			if dataJson["symbol"] != nil {
				thisMessagePair = dataJson["symbol"].(string)
			} else {
				slog.Warn("No 'symbol' field in message", "message", string(rawMessage))
				continue
			}
		}
	} else {
		slog.Warn("No data in message", "message", string(rawMessage))
	}

	slog.Debug("This message pair", "pair", thisMessagePair)

	var datapoints []datamodels.DataPoint
	if incomingMessage.Data != nil {
		datapoints, err = dataToDataPoint(*incomingMessage.Data)
		if err != nil {
			slog.Warn("Error converting message to data point", "error", err, "message", string(rawMessage))
			return
		}
	} else {
		slog.Warn("No data in message", "message", string(rawMessage))
	}

	slog.Debug("Datapoints from message", "datapoints", datapoints)

	// subscribers > 0
	if len(k.subscribers) == 0 {
		slog.Warn("No subscribers for this message", "message", string(rawMessage))
		return
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	for _, subscription := range k.subscribers {
		slog.Debug("Checking subscription", "subscription", subscription.SubscriptionName)
		_, subscriptionDataFeed, subscriptionPair := separateOutboundSubscriptionName(subscription.SubscriptionName)
		if subscriptionDataFeed == thisMessageChannelName && subscriptionPair == thisMessagePair {
			for _, datapoint := range datapoints {
				subscription.DataPointChan <- datapoint
			}
		}
	}
}

func (k *KrakenClient) Close() {

	time.Sleep(100 * time.Millisecond)
	if k.publicConn != nil {
		slog.Info(fmt.Sprintf("%s closing public websocket connection", k.GetName()))
		k.publicConn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(100*time.Millisecond),
		)
		k.publicConn.Close()
	}
	k.publicConn = nil
	if k.privateConn != nil {
		slog.Info(fmt.Sprintf("%s closing private websocket connection", k.GetName()))
		k.privateConn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(100*time.Millisecond),
		)
		k.privateConn.Close()
	}
	k.privateConn = nil
}

func (k *KrakenClient) IsSubscribed(channelName datamodels.KrakenDataChannel, pair string) bool {
	newFeedName := createInboundSubscriptionName(channelName, pair)
	if general.ItemInSlice(k.inboundChannels, newFeedName) {
		slog.Info(fmt.Sprintf("%s is already subscribed to %s", k.GetName(), newFeedName))
		// already subscribed, do nothing
		return true
	}
	return false
}

func krakenV1MessageFromBytes(message []byte) (datamodels.KrakenMessageV1, error) {
	// read message as an array of len 4
	var messageArray []interface{}
	err := json.Unmarshal(message, &messageArray)
	if err != nil {
		return datamodels.KrakenMessageV1{}, err
	}

	if len(messageArray) != 4 {
		return datamodels.KrakenMessageV1{}, errors.Newf("invalid message length: %d", len(messageArray))
	}

	// int-ify the first element
	channelId, ok := messageArray[0].(float64)
	if !ok {
		return datamodels.KrakenMessageV1{}, errors.Newf("invalid channel id: %v", messageArray[0])
	}

	channelName, ok := messageArray[2].(string)
	if !ok {
		return datamodels.KrakenMessageV1{}, errors.Newf("invalid channel name: %v", messageArray[2])
	}

	result := datamodels.KrakenMessageV1{
		ChannelId:   int(channelId),
		Data:        messageArray[1],
		ChannelName: datamodels.KrakenDataChannel(channelName),
		Pair:        messageArray[3].(string),
	}

	return result, nil
}

func krakenTickerSnapshotDataFromInterface(data interface{}) (datamodels.KrakenV1TickerSnapshotData, error) {
	/*
		Need to parse this object: {"a":["69480.20000",12,"12.38898066"],
		"b":["69480.10000",0,"0.00227973"],
		"c":["69480.10000","0.00055826"],
		"v":["1826.71516776","2114.77837537"],
		"p":["69404.24072","69117.95499"],
		"t":[26670,32318],"l":["67481.40000","66836.00000"],
		"h":["70482.00000","70482.00000"],
		"o":["67787.80000","67334.90000"]}
		The mixed types make it difficult to unmarshal directly.
		So we'll unmarshal as a map[string]interface{} first, then extract the values we need.
	*/
	// decode the json
	rawDataMap, ok := data.(map[string]interface{})
	if !ok {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Newf("invalid data format: %v", data)
	}

	// convert mixed types into float64 arrays
	askArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["a"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting ask array")
	}
	bidArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["b"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting bid array")
	}
	closeArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["c"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting close array")
	}
	volumeArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["v"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting volume array")
	}
	volumeWeightedAveragePriceArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["p"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting volume weighted average price array")
	}
	numberOfTradesArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["t"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting number of trades array")
	}
	lowArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["l"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting low array")
	}
	highArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["h"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting high array")
	}
	openArray, err := general.ConvertMixedTypesToFloat64Array(rawDataMap["o"].([]interface{}))
	if err != nil {
		return datamodels.KrakenV1TickerSnapshotData{}, errors.Wrap(err, "error converting open array")
	}

	result := datamodels.KrakenV1TickerSnapshotData{
		Ask: struct {
			BestAsk  float64 `json:"best_ask"`
			WholeLot float64 `json:"whole_lot"`
			Decimal  float64 `json:"decimal"`
		}{BestAsk: askArray[0], WholeLot: askArray[1], Decimal: askArray[2]},
		Bid: struct {
			BestBid  float64 `json:"best_bid"`
			WholeLot float64 `json:"whole_lot"`
			Decimal  float64 `json:"decimal"`
		}{BestBid: bidArray[0], WholeLot: bidArray[1], Decimal: bidArray[2]},
		Close: struct {
			Price  float64 `json:"price"`
			Volume float64 `json:"volume"`
		}{Price: closeArray[0], Volume: closeArray[1]},
		Volume: struct {
			Today       float64 `json:"today"`
			Last24Hours float64 `json:"last_24_hours"`
		}{Today: volumeArray[0], Last24Hours: volumeArray[1]},
		VolumeWeightedAveragePrice: struct {
			Today       float64 `json:"today"`
			Last24Hours float64 `json:"last_24_hours"`
		}{Today: volumeWeightedAveragePriceArray[0], Last24Hours: volumeWeightedAveragePriceArray[1]},
		NumberOfTrades: struct {
			Today       int `json:"today"`
			Last24Hours int `json:"last_24_hours"`
		}{Today: int(numberOfTradesArray[0]), Last24Hours: int(numberOfTradesArray[1])},
		Low: struct {
			Today       float64 `json:"today"`
			Last24Hours float64 `json:"last_24_hours"`
		}{Today: lowArray[0], Last24Hours: lowArray[1]},
		High: struct {
			Today       float64 `json:"today"`
			Last24Hours float64 `json:"last_24_hours"`
		}{Today: highArray[0], Last24Hours: highArray[1]},
		Open: struct {
			Today       float64 `json:"today"`
			Last24Hours float64 `json:"last_24_hours"`
		}{Today: openArray[0], Last24Hours: openArray[1]},
	}

	return result, nil
}

func krakenV1TradesFromTradesResultInterface(data interface{}) ([]datamodels.KrakenV1Trade, error) {
	/*
		Need to parse this object: [[  "5541.20000",  "0.15850568",  "1534614057.321597",  "s",  "l",  ""],[  "6060.00000",  "0.02455000",  "1534614057.324998",  "b",  "l",  ""]]
	*/
	rawDataArray, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data format: %v", data)
	}
	resultArray := make([]datamodels.KrakenV1Trade, 0)

	for _, tradeInterface := range rawDataArray {
		trade, ok := tradeInterface.([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid trade format: %v", tradeInterface)
		}
		if len(trade) != 6 && len(trade) != 7 {
			return nil, fmt.Errorf("invalid trade length: %d", len(trade))
		}
		price, err := strconv.ParseFloat(trade[0].(string), 64)
		if err != nil {
			return nil, err
		}
		volume, err := strconv.ParseFloat(trade[1].(string), 64)
		if err != nil {
			return nil, err
		}
		// see if is float or string
		timestamp, ok := trade[2].(float64)
		if !ok {
			stringTimestamp, ok := trade[2].(string)
			if !ok {
				return nil, fmt.Errorf("can't parse timestamp as float or string: %v", trade[2])
			}
			timestamp, err = strconv.ParseFloat(stringTimestamp, 64)
			if err != nil {
				return nil, err
			}
		}

		side, ok := trade[3].(string)
		if !ok {
			return nil, err
		}
		orderType, ok := trade[4].(string)
		if !ok {
			return nil, err
		}
		miscellaneous, ok := trade[5].(string)
		if !ok {
			return nil, fmt.Errorf("invalid miscellaneous: %v", trade[5])
		}
		resultArray = append(resultArray, datamodels.KrakenV1Trade{
			Price:         price,
			Volume:        volume,
			Timestamp:     timestamp,
			Side:          datamodels.OrderSide(side),
			OrderType:     datamodels.OrderType(orderType),
			Miscellaneous: miscellaneous,
		})
	}

	return resultArray, nil
}

func getDataAsJsonArray(data []interface{}) ([]map[string]interface{}, error) {
	resultArray := make([]map[string]interface{}, 0)

	// then, unmarshall each element as a map
	for _, element := range data {
		elementMap, ok := element.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid element format: %v", element)
		}
		resultArray = append(resultArray, elementMap)
	}
	return resultArray, nil
}

func unmarshalRequestAck(rawMessage []byte, message *datamodels.KrakenRequestAckV2) error {
	err := json.Unmarshal(rawMessage, message)
	if err != nil {
		return err
	}
	// check all the fields aren't null
	if message.Method == "" && message.TimeIn == "" && message.TimeOut == "" {
		return fmt.Errorf("invalid request ack: %v", message)
	}
	return nil
}

func unmarshalIncomingMessage(rawMessage []byte, message *datamodels.KrakenIncomingMessageV2) error {
	err := json.Unmarshal(rawMessage, message)
	if err != nil {
		return err
	}
	if message.Channel == "" {
		return fmt.Errorf("invalid incoming message: %v", message)
	}
	return nil
}

func isPrivateConnectionSubscription(channelName datamodels.KrakenDataChannel) bool {
	return channelName == datamodels.KrakenDataChannelOwnTrades
}

func createInboundSubscriptionName(channelName datamodels.KrakenDataChannel, pair string) string {
	return fmt.Sprintf("%s_%s", channelName, pair)
}

func createOutboundSubscriptionName(subscriberName string, channelName datamodels.KrakenDataChannel, pair string) string {
	return fmt.Sprintf("%s_%s_%s", subscriberName, channelName, pair)
}

func separateOutboundSubscriptionName(subscriptionName string) (string, datamodels.KrakenDataChannel, string) {
	parts := strings.Split(subscriptionName, "_")
	// last two parts are channel and pair
	feedStructName := parts[0]
	dataType := datamodels.KrakenDataChannel(parts[len(parts)-2])
	pair := parts[len(parts)-1]
	return feedStructName, dataType, pair
}

func pairArrayToString(pairs []string) string {
	return strings.Join(pairs, "/")
}

func pairStringToArray(pair string) []string {
	return strings.Split(pair, "/")
}

func krakenPairV2ToPairV1(pair string) string {
	pair = strings.Replace(pair, "BTC", "XBT", 1)
	return strings.Replace(pair, "/", "", 1)
}

func dataToDataPoint(data []interface{}) ([]datamodels.DataPoint, error) {
	// data is an array of json objects, but each object is just bytes when it comes in
	result := make([]datamodels.DataPoint, 0)
	for _, dataInterface := range data {
		dataBytes, err := json.Marshal(dataInterface)
		if err != nil {
			return nil, err
		}
		// unmarshal the data bytes into a map
		var dataMap map[string]interface{}
		err = json.Unmarshal(dataBytes, &dataMap)
		if err != nil {
			return nil, err
		}

		datapoint := &datamodels.GenericDataPoint{
			Elements: make([]datamodels.DataPointElement, 0),
		}
		// for each k,v in the map, add to the datapoint
		for key, value := range dataMap {
			datapoint.Elements = append(datapoint.Elements, datamodels.DataPointElement{
				Field:     key,
				FieldType: datamodels.FieldType(reflect.TypeOf(value).String()),
				Value:     value,
			})
		}
		datapoint.Timestamp = time.Now()
		result = append(result, datapoint)
	}

	return result, nil
}

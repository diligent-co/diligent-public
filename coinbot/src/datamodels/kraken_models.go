package datamodels

import "fmt"

// REST API

type KrakenRestResponse[T any] struct {
	Error  []string `json:"error"`
	Result T        `json:"result"`
}

type KrakenServerTimeResult struct {
	UnixTime int64  `json:"unixtime"`
	Rfc1123  string `json:"rfc1123"`
}

type KrakenSystemStatusResult struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

type KrakenWebSocketsTokenResult struct {
	Token   string `json:"token"`
	Expires int64  `json:"expires"`
}

type KrakenAssetInfoResult struct {
	AssetClass      string `json:"aclass"`
	AltName         string `json:"altname"`
	Decimals        int    `json:"decimals"`
	DisplayDecimals int    `json:"display_decimals"`
	CollateralValue int    `json:"collateral_value"`
	Status          string `json:"status"`
}

type AssetInfoType string

const (
	AssetInfoTypeInfo     AssetInfoType = "info"
	AssetInfoTypeLeverage AssetInfoType = "leverage"
	AssetInfoTypeFees     AssetInfoType = "fees"
	AssetInfoTypeMargin   AssetInfoType = "margin"
)

type TradableAssetPairRequestArgs struct {
	Pair        string        `url:"pair"`
	Info        AssetInfoType `url:"info"`
	CountryCode string        `url:"country_code"`
}

type TradableAssetPairResult struct {
	AltName            string      `json:"altname"`
	WsName             string      `json:"wsname"`
	AssetClassBase     string      `json:"aclass_base"`
	Base               string      `json:"base"`
	AssetClassQuote    string      `json:"aclass_quote"`
	Quote              string      `json:"quote"`
	Lot                string      `json:"lot"`
	CostDecimals       int         `json:"cost_decimals"`
	PairDecimals       int         `json:"pair_decimals"`
	LotDecimals        int         `json:"lot_decimals"`
	LotMultiplier      int         `json:"lot_multiplier"`
	LeverageBuy        []int       `json:"leverage_buy"`
	LeverageSell       []int       `json:"leverage_sell"`
	Fees               [][]float64 `json:"fees"`
	FeesMaker          [][]float64 `json:"fees_maker"`
	FeeVolumeCurrency  string      `json:"fee_volume_currency"`
	MarginCall         int         `json:"margin_call"`
	MarginStop         int         `json:"margin_stop"`
	OrderMin           string      `json:"ordermin"`
	CostMin            float64     `json:"costmin"`
	TickSize           float64     `json:"tick_size"`
	Status             string      `json:"status"`
	LongPositionLimit  string      `json:"longpositionlimit"`
	ShortPositionLimit string      `json:"shortpositionlimit"`
}

type TickerResult struct {
	Ask                        []float64 `json:"a"`
	Bid                        []float64 `json:"b"`
	Last                       []float64 `json:"c"`
	Volume                     []float64 `json:"v"`
	VolumeWeightedAveragePrice []float64 `json:"p"`
	NumberOfTrades             int       `json:"t"`
	Low                        []float64 `json:"l"`
	High                       []float64 `json:"h"`
	Today                      []float64 `json:"o"`
}

type KrakenGetTradesCustomResult struct {
	Pair   string          `json:"pair"`
	Trades []KrakenV1Trade `json:"trades"`
	Last   int64           `json:"last"`
}

// TODO: OHLC, Order Book, Trades, Spreads

// WebSocket models

type KrakenDataChannel string

const (
	KrakenDataChannelStatus    KrakenDataChannel = "status"
	KrakenDataChannelHeartbeat KrakenDataChannel = "heartbeat"
	KrakenDataChannelTicker    KrakenDataChannel = "ticker"
	KrakenDataChannelBook      KrakenDataChannel = "book"
	KrakenDataChannelTrade     KrakenDataChannel = "trade"
	KrakenDataChannelOwnTrades KrakenDataChannel = "ownTrades"
)

type KrakenSubscribeRequestV1 struct {
	Event        string `json:"event"`
	Subscription struct {
		Name  string  `json:"name"`
		Token *string `json:"token"`
	} `json:"subscription"`
	Pair []string `json:"pair"`
}

type KrakenMessageV1 struct {
	// Common fields
	Event       string            `json:"event,omitempty"`
	ChannelId   int               `json:"channelID,omitempty"`
	ChannelName KrakenDataChannel `json:"channelName,omitempty"`
	Status      string            `json:"status,omitempty"`
	Pair        string            `json:"pair,omitempty"`

	// ConnectionAck specific fields
	ConnectionId int64  `json:"connectionID,omitempty"`
	Version      string `json:"version,omitempty"`

	// SubscribeAck specific fields
	Subscription struct {
		Name string `json:"name"`
	} `json:"subscription,omitempty"`

	// IncomingMessage specific fields
	Data interface{} `json:"data,omitempty"`
}

type KrakenV1TickerSnapshotData struct {
	Ask struct {
		BestAsk  float64 `json:"best_ask"`
		WholeLot float64 `json:"whole_lot"`
		Decimal  float64 `json:"decimal"`
	} `json:"a"`
	Bid struct {
		BestBid  float64 `json:"best_bid"`
		WholeLot float64 `json:"whole_lot"`
		Decimal  float64 `json:"decimal"`
	} `json:"b"`
	Close struct {
		Price  float64 `json:"price"`
		Volume float64 `json:"volume"`
	} `json:"c"`
	Volume struct {
		Today       float64 `json:"today"`
		Last24Hours float64 `json:"last_24_hours"`
	} `json:"v"`
	VolumeWeightedAveragePrice struct {
		Today       float64 `json:"today"`
		Last24Hours float64 `json:"last_24_hours"`
	} `json:"p"`
	NumberOfTrades struct {
		Today       int `json:"today"`
		Last24Hours int `json:"last_24_hours"`
	} `json:"t"`
	Low struct {
		Today       float64 `json:"today"`
		Last24Hours float64 `json:"last_24_hours"`
	} `json:"l"`
	High struct {
		Today       float64 `json:"today"`
		Last24Hours float64 `json:"last_24_hours"`
	} `json:"h"`
	Open struct {
		Today       float64 `json:"today"`
		Last24Hours float64 `json:"last_24_hours"`
	} `json:"o"`
}

type KrakenV1Trade struct {
	Price         float64   `json:"price"`
	Volume        float64   `json:"volume"`
	Timestamp     float64   `json:"timestamp"`
	Side          OrderSide `json:"side"`
	OrderType     OrderType `json:"order_type"`
	Miscellaneous string    `json:"miscellaneous"`
}

type KrakenRequestV2 struct {
	Method    string      `json:"method"`
	Params    interface{} `json:"params"`
	RequestId *int        `json:"reqid,omitempty"`
	Token     string      `json:"token,omitempty"`
}

type KrakenRequestAckV2 struct {
	Error   *string `json:"error"`
	Method  string  `json:"method"`
	Success bool    `json:"success"`
	TimeIn  string  `json:"time_in"`
	TimeOut string  `json:"time_out"`
}

func (k *KrakenRequestAckV2) ToString() string {
	if k.Error == nil {
		return fmt.Sprintf("Method: %s, Success: %t, TimeIn: %s, TimeOut: %s", k.Method, k.Success, k.TimeIn, k.TimeOut)
	}
	return fmt.Sprintf("Error: %s, Method: %s, Success: %t, TimeIn: %s, TimeOut: %s", *k.Error, k.Method, k.Success, k.TimeIn, k.TimeOut)
}

type KrakenIncomingMessageV2 struct {
	Channel KrakenDataChannel `json:"channel"`
	Type    *string           `json:"type"`
	Data    *[]interface{}    `json:"data"`
}

type KrakenV2TriggerReference string

const (
	KrakenV2TriggerReferenceLastTrade KrakenV2TriggerReference = "last"
	KrakenV2TriggerReferenceIndex     KrakenV2TriggerReference = "index"
)

type KrakenV2PriceType string

const (
	KrakenV2PriceTypeStatic KrakenV2PriceType = "static"
	KrakenV2PriceTypePct    KrakenV2PriceType = "pct"
	KrakenV2PriceTypeQuote  KrakenV2PriceType = "quote"
)

type KrakenV2Trigger struct {
	Reference KrakenV2TriggerReference `json:"reference"`
	Price     *float64                 `json:"price"`
	PriceType KrakenV2PriceType        `json:"price_type"`
}

type KrakenV2TimeInForce string

const (
	KrakenV2TimeInForceGoodTillCancel    KrakenV2TimeInForce = "gtc"
	KrakenV2TimeInForceGoodTillTime      KrakenV2TimeInForce = "gtd"
	KrakenV2TimeInForceImmediateOrCancel KrakenV2TimeInForce = "ioc"
)

type KrakenV2FeePreference string

const (
	KrakenV2FeePreferenceBase  KrakenV2FeePreference = "base"
	KrakenV2FeePreferenceQuote KrakenV2FeePreference = "quote"
)

type KrakenV2StopType string

const (
	KrakenV2StopTypeCancelNewest KrakenV2StopType = "cancel_newest"
	KrakenV2StopTypeCancelOldest KrakenV2StopType = "cancel_oldest"
	KrakenV2StopTypeCancelBoth   KrakenV2StopType = "cancel_both"
)

type KrakenV2OrderParams struct {
	// https://docs.kraken.com/api/docs/websocket-v2/add_order
	OrderType         OrderType              `json:"order_type"`
	Side              OrderSide              `json:"side"`
	OrderQuantity     float64                `json:"order_qty"`
	Symbol            string                 `json:"symbol"`
	LimitPrice        *float64               `json:"limit_price"`
	LimitPriceType    *KrakenV2PriceType     `json:"limit_price_type"`
	Triggers          *KrakenV2Trigger       `json:"triggers"`
	TimeInForce       *KrakenV2TimeInForce   `json:"time_in_force"`
	Margin            *bool                  `json:"margin"`
	PostOnly          *bool                  `json:"post_only"`
	ReduceOnly        *bool                  `json:"reduce_only"`
	EffectiveTime     *string                `json:"effective_time"` // RFC3339
	ExpireTime        *string                `json:"expire_time"`    // RFC3339
	Deadline          *string                `json:"deadline"`       // RFC3339
	ClientOrderId     *string                `json:"cl_order_id"`
	OrderUserRef      *int                   `json:"order_userref"`
	Conditional       interface{}            `json:"conditional"`
	DisplayQuantity   *float64               `json:"display_qty"`
	FeePreference     *KrakenV2FeePreference `json:"fee_preference"`
	StopType          *KrakenV2StopType      `json:"stp_type"`
	CashOrderQuantity *float64               `json:"cash_order_qty"`
	Validate          *bool                  `json:"validate"` // only validate, do not submit
	SenderSubId       *string                `json:"sender_sub_id"`
	Token             string                 `json:"token"`
}

type KrakenV2CancelOrderParams struct {
	OrderId       []string  `json:"order_id"`
	ClientOrderId *[]string `json:"cl_order_id"`
	OrderUserRef  *[]int    `json:"order_userref"`
	Token         string    `json:"token"`
}

// type Bid struct {
// 	Price    float64 `json:"price"`
// 	Quantity float64 `json:"qty"`
// }

// type Ask struct {
// 	Price    float64 `json:"price"`
// 	Quantity float64 `json:"qty"`
// }

// type BookSnapshotData struct {
// 	Symbol   string `json:"symbol"`
// 	Bids     []Bid  `json:"bids"`
// 	Asks     []Ask  `json:"asks"`
// 	Checksum int    `json:"checksum"`
// }

// type SnapshotResponse struct {
// 	Channel string      `json:"channel"`
// 	Type    string      `json:"type"`
// 	Data    interface{} `json:"data"`
// }

// type TradeRequest struct {
// 	Method    string      `json:"method"`
// 	Params    interface{} `json:"params"`
// 	RequestId int         `json:"reqid"`
// }

// type TradeResponse struct {
// 	Method  string      `json:"method"`
// 	Result  interface{} `json:"result"`
// 	Success bool        `json:"success"`
// 	TimeIn  string      `json:"time_in"`
// 	TimeOut string      `json:"time_out"`
// }

// type AddOrderRequestParams struct {
// 	OrderType     OrderType `json:"order_type"`
// 	Side          OrderSide `json:"side"`
// 	LimitPrice    float64   `json:"limit_price"`
// 	OrderUserRef  string    `json:"order_userref"`
// 	OrderQuantity float64   `json:"order_qty"`
// 	Symbol        string    `json:"symbol"`
// 	Token         string    `json:"token"`
// }

// type AmendOrderRequestParams struct {
// 	ClientOrderId string  `json:"cl_order_id"`
// 	LimitPrice    float64 `json:"limit_price"`
// 	OrderQuantity float64 `json:"order_qty"`
// 	Token         string  `json:"token"`
// }

// type CancelOrderRequestParams struct {
// 	OrderId []string `json:"order_id"`
// 	Token   string   `json:"token"`
// }

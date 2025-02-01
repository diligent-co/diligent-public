package database

import "coinbot/src/datamodels"

var DbTables = []interface{}{
	&datamodels.CoinbotTrade{},
	&datamodels.KrakenTransactionHistory{},
	&datamodels.FeedValue{},
	&datamodels.AggregatorValue{},
}

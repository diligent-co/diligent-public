package database

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"coinbot/src/datamodels"
)

type TransactionDb interface {
	WriteTrade(ctx context.Context, trade datamodels.CoinbotTrade) error
	WriteTransactionHistory(ctx context.Context, transactionSet datamodels.TransactionSet) error
}

func (d *databaseImplementation) WriteTrade(
	ctx context.Context,
	trade datamodels.CoinbotTrade) error {
	return d.gormDb.Create(&trade).Error
}

func (d *databaseImplementation) GetTrades(
	ctx context.Context,
	startTime time.Time,
	endTime time.Time,
	symbol *string,
	orderSide *datamodels.OrderSide) ([]datamodels.CoinbotTrade, error) {

	query := d.gormDb.WithContext(ctx).Model(&datamodels.CoinbotTrade{})

	if symbol != nil {
		query = query.Where("symbol = ?", *symbol)
	}
	if orderSide != nil {
		query = query.Where("order_side = ?", *orderSide)
	}
	query = query.Where("timestamp BETWEEN ? AND ?", startTime, endTime)

	var trades []datamodels.CoinbotTrade
	if err := query.Find(&trades).Error; err != nil {
		return nil, err
	}
	return trades, nil
}

func (d *databaseImplementation) WriteTransactionHistory(
	ctx context.Context,
	transactionSet datamodels.TransactionSet) error {

	batchSize := 5000
	// check which implementation of TransactionSet we are dealing with
	transactionSetType := reflect.TypeOf(transactionSet)
	switch transactionSetType {
	default:
		return errors.New("unknown transaction set type")
	case reflect.TypeOf(&datamodels.KrakenTransactionSet{}):
		// cast the transaction set to a KrakenTransactionSet
		castTransactionSet, ok := transactionSet.(*datamodels.KrakenTransactionSet)
		if !ok {
			return errors.New("failed to cast transaction set to KrakenTransactionSet")
		}
		// create a slice of the correct type

		transactions := castTransactionSet.Transactions

		tx := d.gormDb.WithContext(ctx).CreateInBatches(transactions, batchSize)
		if tx.Error != nil {
			slog.Error("Error inserting transaction history", "error", tx.Error)
			return tx.Error
		}
	}

	return nil
}

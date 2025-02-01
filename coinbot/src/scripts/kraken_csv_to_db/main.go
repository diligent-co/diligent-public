package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"coinbot/src/config"
	"coinbot/src/database"
	"coinbot/src/datamodels"
	"coinbot/src/utils/general"
)

// huge folder of csv files, each of which is titled by pair (sym1sym2)
// some syms have three letters, some have four
// each file has 3 columns, timestamp, sym2_quantity, sym1_quantity
// timestamp is in unix time
// sym2_quantity and sym1_quantity are floats

// we want to insert into the database the following:
// timestamp, sym1, sym2, sym1_quantity, sym2_quantity

func main() {
	// verify that there are two command line args
	if len(os.Args) != 2 {
		slog.Error("Usage: kraken_csv_to_db <data_dir>")
		os.Exit(1)
	}
	// data dir from command line args
	dataDir := os.Args[1]
	if dataDir == "" {
		slog.Error("No data directory provided")
		os.Exit(1)
	}
	// verify dir exists
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		slog.Error("Data directory does not exist", "error", err)
		os.Exit(1)
	}
	// verify dir contains only csv files
	files, err := filepath.Glob(filepath.Join(dataDir, "*.csv"))
	if err != nil {
		slog.Error("Failed to get files in data directory", "error", err)
		os.Exit(1)
	}

	config, err := config.Load()
	if err != nil {
		slog.Error("Failed to get coinbot config", "error", err)
		os.Exit(1)
	}
	db, err := database.NewDBConnection(config.DatabaseConfig)
	if err != nil {
		slog.Error("Failed to connect to postgres", "error", err)
		os.Exit(1)
	}

	// open symbols.json, navigating to current dir from caller()
	_, thisFileDir, _, ok := runtime.Caller(0)
	if !ok {
		slog.Error("Failed to get current directory")
		os.Exit(1)
	}
	// 4 levels up from this file
	symbolsFilePath := filepath.Join(thisFileDir, "..", "..", "..", "..", "symbols.json")
	symbolsFile, err := os.Open(symbolsFilePath)
	if err != nil {
		slog.Error("Failed to open symbols.json", "error", err)
		os.Exit(1)
	}
	defer symbolsFile.Close()
	symbolsFileBytes, err := io.ReadAll(symbolsFile)
	if err != nil {
		slog.Error("Failed to read symbols.json", "error", err)
		os.Exit(1)
	}
	var symbols map[string][]string
	err = json.Unmarshal(symbolsFileBytes, &symbols)
	if err != nil {
		slog.Error("Failed to unmarshal symbols.json", "error", err)
		os.Exit(1)
	}
	symbolKeys := make([]string, 0)
	for key := range symbols {
		symbolKeys = append(symbolKeys, key)
	}

	// check how many files there are
	numFiles := len(files)
	slog.Info("Found files", "num_files", numFiles)
	errors := make([]string, 0)
	// check how many files have 6 letters, and how many have 7/8
	for _, file := range files {
		fileNameWithoutExt := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		if !general.ItemInSlice(symbolKeys, fileNameWithoutExt) {
			slog.Error("File not found in symbols.json", "file", fileNameWithoutExt)
			errors = append(errors, fileNameWithoutExt)
		}
	}
	if len(errors) > 0 {
		slog.Error("Files not found in symbols.json", "files", errors)
		os.Exit(1)
	}

	// for each file, read the file, parse the csv, and insert into the database
	for _, file := range files {
		theseTransactions, err := parseKrakenCsv(file, symbols)
		if err != nil {
			slog.Error("Failed to parse kraken csv", "error", err)
			os.Exit(1)
		}
		transactionSet := datamodels.KrakenTransactionSet{
			Transactions: theseTransactions,
		}

		transactionCount := strconv.Itoa(len(theseTransactions))
		sym1 := theseTransactions[0].Symbol1
		sym2 := theseTransactions[0].Symbol2

		slog.Info("Writing %d transactions between %s and %s for file %s", transactionCount, sym1, sym2, file)
		// _ = db
		// _ = transactionSet
		// _ = context.Background()

		db.WriteTransactionHistory(context.Background(), &transactionSet)
	}

	os.Exit(0)
}

func parseKrakenCsv(file string, symbols map[string][]string) ([]datamodels.KrakenTransactionHistory, error) {

	// open the file
	csvFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.Comma = ','
	csvReader.LazyQuotes = true

	lines, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	// get sym1 and sym2 from the file name
	fileNameWithoutExt := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
	theseSymbols := symbols[fileNameWithoutExt]
	if len(theseSymbols) != 2 {
		return nil, fmt.Errorf("expected 2 symbols, got %d, for key %s", len(theseSymbols), fileNameWithoutExt)
	}
	sym1 := theseSymbols[0]
	sym2 := theseSymbols[1]

	transactionHistory := make([]datamodels.KrakenTransactionHistory, 0)
	for _, line := range lines {
		// each line has 3 columns, timestamp, sym2_quantity, sym1_quantity
		// timestamp is in unix time
		// sym2_quantity and sym1_quantity are floats
		lineTimeInt, err := strconv.ParseInt(line[0], 10, 64)
		if err != nil {
			return nil, err
		}
		lineTime := time.Unix(lineTimeInt, 0)

		lineSym2Quantity, err := strconv.ParseFloat(line[1], 64)
		if err != nil {
			return nil, err
		}

		lineSym1Quantity, err := strconv.ParseFloat(line[2], 64)
		if err != nil {
			return nil, err
		}

		krakenTransactionHistory := datamodels.KrakenTransactionHistory{
			Timestamp:     lineTime,
			Symbol1:       sym1,
			Symbol2:       sym2,
			Symbol1Amount: lineSym1Quantity,
			Symbol2Amount: lineSym2Quantity,
		}
		transactionHistory = append(transactionHistory, krakenTransactionHistory)

	}

	return transactionHistory, nil
}

package datamodels

import (
	"log/slog"
	"path/filepath"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/viper"

	"coinbot/src/utils/errors"
)

type CoinbotConfig struct {
	DatabaseConfig PostgresConfig `mapstructure:"postgres"`
	KrakenConfig   KrakenConfig   `mapstructure:"kraken"`
	StorageConfig  StorageConfig  `mapstructure:"storage"`
	SymbolsConfig  SymbolsConfig  `mapstructure:"symbols"`
	ServerConfig   ServerConfig   `mapstructure:"server"`
}

type PostgresConfig struct {
	Database string `mapstructure:"database"`
	Host     string `mapstructure:"host"`
	Password string `mapstructure:"password"`
	Port     int    `mapstructure:"port"`
	SSL      struct {
		CA   string `mapstructure:"ca"`
		Cert string `mapstructure:"cert"`
		Key  string `mapstructure:"key"`
		Mode string `mapstructure:"mode"`
	} `mapstructure:"ssl"`
	URI  string `mapstructure:"uri"`
	User string `mapstructure:"user"`
}

type KrakenConfig struct {
	APIKey    string `mapstructure:"api_key"`
	APISecret string `mapstructure:"api_secret"`
	WsVersion int    `mapstructure:"ws_version"`
}

type SymbolsConfig struct {
	FilePath string `mapstructure:"file_path"`
}

type ServerConfig struct {
	Port            string `mapstructure:"port"`
	MetricsEndpoint string `mapstructure:"metrics_endpoint"`
	HealthEndpoint  string `mapstructure:"health_endpoint"`
}

type WSConfig struct {
	Upgrader websocket.Upgrader
}

type StorageConfig struct {
	Bucket       string `mapstructure:"bucket"`
	Region       string `mapstructure:"region"`
	KrakenPrefix string `mapstructure:"kraken_prefix"`
}

//// end server configs

type CsvColumnConfig struct {
	ColumnIndex int       `mapstructure:"column_index"`
	FieldName   string    `mapstructure:"field_name"`
	FieldType   FieldType `mapstructure:"field_type"`
}

type CsvFeedConfig struct {
	FilePath         string            `mapstructure:"file_path"`
	RelationName     string            `mapstructure:"relation_name"`
	Asset            Asset             `mapstructure:"asset"`
	TimestampColName string            `mapstructure:"timestamp_col_name"`
	Columns          []CsvColumnConfig `mapstructure:"columns"`
}

type LiveFeedConfig struct {
	Asset           Asset `mapstructure:"asset"`
	WriteToDatabase bool  `mapstructure:"write_to_database"`
}

// FeedConfig contains parameters for constructing a data feed
type FeedConfig struct {
	FeedName       string          `mapstructure:"feed_name"`
	TickInterval   time.Duration   `mapstructure:"tick_interval"`
	StartTime      string          `mapstructure:"start_time"`
	EndTime        string          `mapstructure:"end_time"`
	InterReadDelay time.Duration   `mapstructure:"inter_read_delay"`
	IsLive         bool            `mapstructure:"is_live"`
	CsvFeedConfig  *CsvFeedConfig  `mapstructure:"csv_feed_config"`
	LiveFeedConfig *LiveFeedConfig `mapstructure:"live_feed_config"`
}

// AggregatorConfig defines parameters for constructing an aggregator
type AggregatorConfig struct {
	Name           string         `mapstructure:"name"`
	AggregatorType AggregatorType `mapstructure:"aggregator_type"` // e.g., "price", "volume"
	InputFeedName  string         `mapstructure:"input_feed_name"`
	WindowSize     int64          `mapstructure:"window_size_seconds"`
	EmitCadence    int64          `mapstructure:"emit_cadence_seconds"`
	AggregatorFunc string         `mapstructure:"aggregator_func"` // e.g., "mean", "stddev", "last", "constant"
	InputFields    []string       `mapstructure:"input_fields"`
	OutputFields   []string       `mapstructure:"output_fields"`
	ConstantValue  float64        `mapstructure:"constant_value"` // used only for constant value aggregators
}

type MetricsWriterConfig struct {
	WsWriter   bool   `mapstructure:"ws_writer"`
	FileWriter bool   `mapstructure:"file_writer"`
	FilePath   string `mapstructure:"file_path"`
}

type ExternalSupplierConfig struct {
	Name         string   `yaml:"name"`
	Host         string   `yaml:"host"`
	Port         int      `yaml:"port"`
	Endpoint     string   `yaml:"endpoint"`
	InputFields  []string `yaml:"input_fields"`  // fields needed from aggregators
	OutputFields []string `yaml:"output_fields"` // fields returned by service
	Timeout      time.Duration `yaml:"timeout"`       // timeout for requests
}

// StrategyConfig contains parameters for constructing a strategy engine
type StrategyConfig struct {
	Type          string                `mapstructure:"type"` // e.g., "simple_momentum"
	StepCadence   time.Duration         `mapstructure:"step_cadence"`
	IsRealTime    bool                  `mapstructure:"is_real_time"`
	Aggregators   []AggregatorConfig    `mapstructure:"aggregators"`
	MetricsWriter *MetricsWriterConfig  `mapstructure:"metrics_writer"`
	SignalFunc    *SignalFunctionConfig `mapstructure:"signal_func"`
}

// PortfolioConfig contains all configuration needed to construct a portfolio
type PortfolioConfig struct {
	// Map of aggregator name to its config
	Strategy StrategyConfig `mapstructure:"strategy"`
	// Portfolio specific settings
	InitialBalance float64              `mapstructure:"initial_balance"`
	Assets         []Asset              `mapstructure:"assets"`
	StartTime      string               `mapstructure:"start_time"`
	EndTime        string               `mapstructure:"end_time"`
	DataDir        string               `mapstructure:"data_dir"`
	MetricsWriter  *MetricsWriterConfig `mapstructure:"metrics_writer"`
}

func (p *PortfolioConfig) Validate() error {
	if p.DataDir == "" {
		return errors.New("data_dir is required")
	}
	if p.StartTime == "" {
		return errors.New("start_time is required")
	}
	if p.EndTime == "" {
		return errors.New("end_time is required")
	}

	if len(p.Assets) == 0 {
		return errors.New("assets are required")
	}

	if p.InitialBalance <= 0 {
		return errors.New("initial_balance must be greater than 0")
	}

	return nil
}

func NewPortfolioConfig(thisFilepath string, baseDir string) (*PortfolioConfig, error) {
	v := viper.New()
	v.SetConfigFile(thisFilepath)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var portfolioConfig PortfolioConfig
	if err := v.Unmarshal(&portfolioConfig); err != nil {
		return nil, err
	}

	// update data_dir to be relative to baseDir
	modifiedDataDir := filepath.Join(baseDir, portfolioConfig.DataDir)
	portfolioConfig.DataDir = modifiedDataDir

	// update metrics file path to be relative to baseDir
	if portfolioConfig.MetricsWriter != nil {
		modifiedMetricsFilePath := filepath.Join(baseDir, portfolioConfig.MetricsWriter.FilePath)
		portfolioConfig.MetricsWriter.FilePath = modifiedMetricsFilePath
	}
	if portfolioConfig.MetricsWriter != nil && portfolioConfig.Strategy.MetricsWriter == nil {
		// copy portfolio metrics writer config to strategy, if strategy metrics writer is not set
		slog.Info("Copying portfolio metrics writer config to strategy")
		portfolioConfig.Strategy.MetricsWriter = portfolioConfig.MetricsWriter
	}

	return &portfolioConfig, nil
}

type SignalFunctionConfig struct {
	Type             string                  `yaml:"type"`
	Assets           []string                `yaml:"assets"`
	ExternalSupplier *ExternalSupplierConfig `yaml:"external_supplier,omitempty"`
}

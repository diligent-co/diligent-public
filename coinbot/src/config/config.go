package config

import (
	"os"
	"path/filepath"

	"coinbot/src/utils/general"
	"coinbot/src/datamodels"

	"github.com/spf13/viper"
)



func Load() (*datamodels.CoinbotConfig, error) {
	// read config path from env var
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		currentDir := general.GetCurrentDir()
		// go up two levels to coinbot directory
		configPath = filepath.Join(currentDir, "..", "..", "config.local.yaml")
	}

	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var coinbotConfig datamodels.CoinbotConfig
	if err := viper.Unmarshal(&coinbotConfig); err != nil {
		return nil, err
	}

	// PrintConfig(&coinbotConfig)

	return &coinbotConfig, nil
}

package symbols

import (
	"coinbot/src/datamodels"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNewSymbolsDictionaryFromConfig(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	thisDirName := filepath.Dir(thisFile)
	symbolsFilePath := thisDirName + "/../../../symbols.json"
	testConfig := &datamodels.SymbolsConfig{
		FilePath: symbolsFilePath,
	}

	symbols := NewSymbolsDictionaryFromConfig(testConfig)

	// verify that XBTUSD maps to BTC/USD and vice versa
	if symbols.V1ToV2["XBTUSD"] != "BTC/USD" {
		t.Errorf("XBTUSD should map to BTC/USD, got %s", symbols.V1ToV2["XBTUSD"])
	}

	if symbols.V2ToV1["BTC/USD"] != "XBTUSD" {
		t.Errorf("BTC/USD should map to XBTUSD, got %s", symbols.V2ToV1["BTC/USD"])
	}
}

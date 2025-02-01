package symbols

import (
	"coinbot/src/datamodels"
	"coinbot/src/utils/errors"
	"encoding/json"
	"os"
	"strings"
)

type SymbolsDictionary struct {
	V1ToV2 map[string]string
	V2ToV1 map[string]string
}

func NewSymbolsDictionaryFromConfig(config *datamodels.SymbolsConfig) *SymbolsDictionary {
	// read in file as json
	file, err := os.Open(config.FilePath)
	if err != nil {
		errors.Wrap(err, "failed to open symbols file")
	}
	defer file.Close()

	// parse json
	symbols := make(map[string][]string)
	err = json.NewDecoder(file).Decode(&symbols)
	if err != nil {
		errors.Wrap(err, "failed to parse symbols file")
	}

	// file is v1 to v2, with v2 needing to be concated to frm sym1/sym2
	v1ToV2 := make(map[string]string)
	v2ToV1 := make(map[string]string)
	for v1, v2 := range symbols {
		// if v1 is XBTSYM2, then we need to convert it to BTC/SYM2
		if strings.HasPrefix(v1, "XBT") {
			v2[0] = "BTC"
		}
		v1ToV2[v1] = strings.Join(v2, "/")
		v2ToV1[strings.Join(v2, "/")] = v1
	}

	return &SymbolsDictionary{
		V1ToV2: v1ToV2,
		V2ToV1: v2ToV1,
	}
}

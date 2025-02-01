package strategies

import (
	"coinbot/src/datamodels"
	"log/slog"
	"strings"
	"time"
)

// SignalFunc is a function that takes a map of aggregator names to their data points
// and returns a signal
type SignalFunc func(map[string][]datamodels.DataPoint) *datamodels.Signal

type GetInputFieldsFunc func() []string

type GetNameFunc func() string

type SignalFunctionSupplier interface {
	GetSignalFunc() SignalFunc
	GetName() string
	GetInputFields() []string
}

type GenericSignalFunctionSupplier struct {
	SignalFunc         SignalFunc
	GetNameFunc        GetNameFunc
	GetInputFieldsFunc GetInputFieldsFunc
	Pairs              []string
}

func NewSignalFunctionSupplier(
	signalFunc SignalFunc,
	getNameFunc GetNameFunc,
	getInputFieldsFunc GetInputFieldsFunc) GenericSignalFunctionSupplier {
	return GenericSignalFunctionSupplier{
		SignalFunc:         signalFunc,
		GetNameFunc:        getNameFunc,
		GetInputFieldsFunc: getInputFieldsFunc,
	}
}

func (s *GenericSignalFunctionSupplier) GetName() string {
	return s.GetNameFunc()
}

func (s *GenericSignalFunctionSupplier) GetInputFields() []string {
	return s.GetInputFieldsFunc()
}

func (s *GenericSignalFunctionSupplier) SetInputFields(inputFields []string) {
	s.GetInputFieldsFunc = func() []string {
		return inputFields
	}
}

func (s *GenericSignalFunctionSupplier) GetPairs() []string {
	return s.Pairs
}

func (s *GenericSignalFunctionSupplier) GetSignalFunc() SignalFunc {
	return s.SignalFunc
}

// / Simple Momentum Strategy ///
func GetSimpleMomentumSignalSupplier(assets []datamodels.Asset) SignalFunctionSupplier {
	supplier := NewSignalFunctionSupplier(
		simpleMomentumSignalFunc,
		simpleMomentumSignalFunctionGetName,
		func() []string {
			return generateMomentumInputFields(assets)
		},
	)
	return &supplier
}

func generateMomentumInputFields(assets []datamodels.Asset) []string {
	inputFields := []string{}
	for _, asset := range assets {
		inputFields = append(inputFields, string(asset)+"_ma_short", string(asset)+"_ma_long", string(asset)+"_volatility")
	}
	return inputFields
}

// get name for simple momentum strategy
func simpleMomentumSignalFunctionGetName() string {
	return "simple_momentum"
}

// SimpleMomentumSignalFunc generates a signal based on momentum and volatility
// data is a map of PAIR_metric to []DataPoint, where metric is ma_short, ma_long, or volatility
// each datapoint is a point in time, and we want to get the last point in time for each pair
func simpleMomentumSignalFunc(data map[string][]datamodels.DataPoint) *datamodels.Signal {
	if len(data) == 0 {
		return nil
	}

	// Calculate momentum and volatility scores for each asset
	scores := make(map[datamodels.Asset]float64)
	totalScore := 0.0

	// Find latest timestamp across all data points
	var firstDataPointTime time.Time = time.Now().Add(-1 * time.Hour)
	var lastDataPointTime time.Time = time.Unix(0, 0)

	// name is PAIR_ma_short or PAIR_ma_long or PAIR_volatility
	// we want to group them by PAIR and get the last point for each
	// the fieldName might be the metric ('price', 'ma_short'),
	//or it might be the asset_metric ('XBT_price', 'XBT_ma_short')
	pairPoints := make(map[string]datamodels.DataPoint)
	for name, points := range data {
		pair := strings.Split(name, "_")[0]
		metric := strings.Join(strings.Split(name, "_")[1:], "_")
		// see if dp already exists for this pair, if not, add it
		// if so, update it with new element
		if dp, ok := pairPoints[pair]; ok {
			if points[len(points)-1].GetTimestamp().After(lastDataPointTime) {
				lastDataPointTime = points[len(points)-1].GetTimestamp()
			}
			if points[len(points)-1].GetTimestamp().Before(firstDataPointTime) {
				firstDataPointTime = points[len(points)-1].GetTimestamp()
			}
			fieldVal, err := points[len(points)-1].GetFieldValue(metric)
			if err != nil {
				continue
			}
			dp.AddElement(metric, datamodels.FieldTypeFloat, fieldVal)
		} else {
			pairPoints[pair] = points[len(points)-1]
		}
	}

	// for each pair, we want to calculate the score
	for pair, point := range pairPoints {
		maShort, err := point.Get("ma_short")
		if err != nil {
			slog.Error("failed to get ma_short for pair", "pair", pair, "error", err)
			continue
		}
		maShortFloat, err := maShort.GetTypedValue()
		if err != nil {
			slog.Error("failed to get ma_short for pair", "pair", pair, "error", err)
			continue
		}
		maLong, err := point.Get("ma_long")
		if err != nil {
			slog.Error("failed to get ma_long for pair", "pair", pair, "error", err)
			continue
		}
		maLongFloat, err := maLong.GetTypedValue()
		if err != nil {
			slog.Error("failed to get ma_long for pair", "pair", pair, "error", err)
			continue
		}
		volatility, err := point.Get("volatility")
		if err != nil {
			slog.Error("failed to get volatility for pair", "pair", pair, "error", err)
			continue
		}
		volatilityFloat, err := volatility.GetTypedValue()
		if err != nil {
			slog.Error("failed to get volatility for pair", "pair", pair, "error", err)
			continue
		}

		momentum := maShortFloat.(float64) / maLongFloat.(float64)
		volatilityPenalty := 1.0 / (1.0 + volatilityFloat.(float64))
		score := momentum * volatilityPenalty
		asset := datamodels.Asset(pair)
		scores[asset] = score
		totalScore += score
	}

	// Normalize scores to create allocation weights
	allocations := make(map[datamodels.Asset]float64)
	for asset, score := range scores {
		if totalScore > 0 {
			allocations[asset] = score / totalScore
		} else {
			allocations[asset] = 1.0 / float64(len(scores)) // Equal weight fallback
		}
	}

	return &datamodels.Signal{
		SignalWriteTime: time.Now(),
		StreamTime:      lastDataPointTime,
		SignalType:      datamodels.SignalTypeAllocation,
		Allocations:     allocations,
	}
}

// EqualizeAllocationsSignalFunc equalizes the allocations across all pairs
func GetDummySignalSupplier(pairs []string) SignalFunctionSupplier {

	supplier := NewSignalFunctionSupplier(
		DummySignalFunc,
		dummySignalFunctionGetName,
		dummySignalFunctionGetInputFields,
	)
	supplier.Pairs = pairs
	return &supplier
}

func GetSingleAssetDummySignalSupplier(pairs []string) SignalFunctionSupplier {

	supplier := NewSignalFunctionSupplier(
		singleAssetDummySignalFunc,
		dummySignalFunctionGetName,
		dummySignalFunctionGetInputFields,
	)
	supplier.Pairs = pairs
	return &supplier
}

// we want to make sure that the sum of the allocations is 1.0
// and that each allocation is proportional to the volume of the pair
func DummySignalFunc(data map[string][]datamodels.DataPoint) *datamodels.Signal {
	// get last timestamp from data
	lastTimestamp := time.Unix(0, 0)
	for _, points := range data {
		if len(points) > 0 && points[len(points)-1].GetTimestamp().After(lastTimestamp) {
			lastTimestamp = points[len(points)-1].GetTimestamp()
		}
	}

	allocations := make(map[datamodels.Asset]float64)
	allocations[datamodels.ETH] = 0.5
	allocations[datamodels.XBT] = 0.5

	signal := &datamodels.Signal{
		StreamTime:  lastTimestamp,
		SignalType:  datamodels.SignalTypeAllocation,
		Allocations: allocations,
	}
	return signal
}

func singleAssetDummySignalFunc(data map[string][]datamodels.DataPoint) *datamodels.Signal {
	// get last timestamp from data
	lastTimestamp := time.Unix(0, 0)
	for _, points := range data {
		if len(points) > 0 && points[len(points)-1].GetTimestamp().After(lastTimestamp) {
			lastTimestamp = points[len(points)-1].GetTimestamp()
		}
	}

	allocations := make(map[datamodels.Asset]float64)
	//allocations[datamodels.USD] = 0.5
	allocations[datamodels.XBT] = 1

	signal := &datamodels.Signal{
		StreamTime:  lastTimestamp,
		SignalType:  datamodels.SignalTypeAllocation,
		Allocations: allocations,
	}
	slog.Info("singleAssetDummySignalFunc", "allocations", allocations)
	return signal
}

func dummySignalFunctionGetName() string {
	return "dummy"
}

func dummySignalFunctionGetInputFields() []string {
	return []string{}
}

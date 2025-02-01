package strategies

import (
	"coinbot/src/datamodels"
	"testing"
	"time"
)

func TestSimpleMomentumSignalFunc(t *testing.T) {
	// Setup test data
	now := time.Now()
	testData := map[string][]datamodels.DataPoint{
		"BTC_ma_short": {
			&datamodels.GenericDataPoint{Timestamp: now,
				Elements: []datamodels.DataPointElement{
					{Field: "ma_short", FieldType: datamodels.FieldTypeFloat, Value: 110.0},
				},
			},
		},
		"BTC_ma_long": {
			&datamodels.GenericDataPoint{Timestamp: now,
				Elements: []datamodels.DataPointElement{
					{Field: "ma_long", FieldType: datamodels.FieldTypeFloat, Value: 100.0},
				},
			},
		},
		"BTC_volatility": {
			&datamodels.GenericDataPoint{Timestamp: now,
				Elements: []datamodels.DataPointElement{
					{Field: "volatility", FieldType: datamodels.FieldTypeFloat, Value: 0.2},
				},
			},
		},
		"ETH_ma_short": {
			&datamodels.GenericDataPoint{Timestamp: now,
				Elements: []datamodels.DataPointElement{
					{Field: "ma_short", FieldType: datamodels.FieldTypeFloat, Value: 90.0},
				},
			},
		},
		"ETH_ma_long": {
			&datamodels.GenericDataPoint{Timestamp: now,
				Elements: []datamodels.DataPointElement{
					{Field: "ma_long", FieldType: datamodels.FieldTypeFloat, Value: 100.0},
				},
			},
		},
		"ETH_volatility": {
			&datamodels.GenericDataPoint{Timestamp: now,
				Elements: []datamodels.DataPointElement{
					{Field: "volatility", FieldType: datamodels.FieldTypeFloat, Value: 0.1},
				},
			},
		},
	}

	// Execute function
	signal := simpleMomentumSignalFunc(testData)

	// Verify results
	if signal == nil {
		t.Fatal("Expected non-nil signal")
	}

	if signal.SignalType != datamodels.SignalTypeAllocation {
		t.Errorf("Expected signal type %v, got %v", datamodels.SignalTypeAllocation, signal.SignalType)
	}

	if len(signal.Allocations) != 2 {
		t.Errorf("Expected 2 allocations, got %d", len(signal.Allocations))
	}

	// BTC has higher momentum (1.1) and higher volatility (0.2)
	// ETH has lower momentum (0.9) and lower volatility (0.1)
	// Check that BTC has higher allocation due to momentum despite volatility penalty
	if signal.Allocations["BTC"] <= signal.Allocations["ETH"] {
		t.Errorf("Expected BTC allocation (%f) to be higher than ETH allocation (%f)",
			signal.Allocations["BTC"], signal.Allocations["ETH"])
	}

	// Verify allocations sum to approximately 1.0
	totalAllocation := 0.0
	for _, allocation := range signal.Allocations {
		totalAllocation += allocation
	}
	if totalAllocation < 0.99 || totalAllocation > 1.01 {
		t.Errorf("Expected total allocation to be approximately 1.0, got %f", totalAllocation)
	}
}

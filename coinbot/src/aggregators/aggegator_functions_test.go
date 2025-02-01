//go:build unit

package aggregators

import (
	"coinbot/src/datamodels"
	"testing"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/assert"
)

func TestConstantOneFunc(t *testing.T) {
	points := createTestPoints()
	constantOneFunc := CreateConstantValueAggregatorFunc([]string{"price"}, []string{"constant_price"}, 1.0)
	result, err := constantOneFunc(points, []string{"price"}, []string{"constant_price"})
	if err != nil {
		t.Errorf("ConstantOneFunc returned unexpected error: %v", err)
	}

	constantVal, err := result.GetFieldValue("constant_price")
	if err != nil {
		t.Errorf("ConstantOneFunc returned error getting value: %v", err)
	}
	assert.Equal(t, 1.0, constantVal)

	constantTwoFunc := CreateConstantValueAggregatorFunc([]string{"price"}, []string{"constant_price"}, 2.0)
	result, err = constantTwoFunc(points, []string{"price"}, []string{"constant_price"})
	if err != nil {
		t.Errorf("ConstantTwoFunc returned unexpected error: %v", err)
	}
	constantVal, err = result.GetFieldValue("constant_price")
	if err != nil {
		t.Errorf("ConstantTwoFunc returned error getting value: %v", err)
	}
	assert.Equal(t, 2.0, constantVal)
}

func TestMeanFunc(t *testing.T) {
	// send in a bunch of points, and make sure the average is correct

	testPrices := []float64{100.0, 101.0, 102.0}

	inputPoints := []datamodels.DataPoint{}
	for _, price := range testPrices {
		inputPoints = append(inputPoints, &datamodels.GenericDataPoint{
			Timestamp: time.Now(),
			Elements:  []datamodels.DataPointElement{{Field: "price", Value: price, FieldType: datamodels.FieldTypeFloat}},
		})
	}

	result, err := MeanFunc(inputPoints, []string{"price"}, []string{"mean_price"})
	if err != nil {
		t.Errorf("Error getting average price: %v", err)
	}

	price, err := result.GetFieldValue("mean_price")
	if err != nil {
		t.Errorf("Error getting price field: %v", err)
	}

	assert.Equal(t, 101.0, price)
}

// Helper function to create test data points
func createTestPoints() []datamodels.DataPoint {
	now := time.Now()
	return []datamodels.DataPoint{
		&datamodels.GenericDataPoint{
			Timestamp: now,
			Elements: []datamodels.DataPointElement{
				{Field: "price", Value: 10.0, FieldType: datamodels.FieldTypeFloat},
				{Field: "volume", Value: 100.0, FieldType: datamodels.FieldTypeFloat},
			},
		},
		&datamodels.GenericDataPoint{
			Timestamp: now.Add(time.Second),
			Elements: []datamodels.DataPointElement{
				{Field: "price", Value: 20.0, FieldType: datamodels.FieldTypeFloat},
				{Field: "volume", Value: 200.0, FieldType: datamodels.FieldTypeFloat},
			},
		},
		&datamodels.GenericDataPoint{
			Timestamp: now.Add(2 * time.Second),
			Elements: []datamodels.DataPointElement{
				{Field: "price", Value: 30.0, FieldType: datamodels.FieldTypeFloat},
				{Field: "volume", Value: 300.0, FieldType: datamodels.FieldTypeFloat},
			},
		},
	}
}

func TestMaxFunc(t *testing.T) {
	points := createTestPoints()
	result, err := MaxFunc(points, []string{"price"}, []string{"max_price"})

	if err != nil {
		t.Errorf("MaxFunc returned unexpected error: %v", err)
	}

	maxVal, err := result.GetFieldValue("max_price")
	if err != nil || maxVal != 30.0 {
		t.Errorf("MaxFunc = %v, want 30.0", maxVal)
	}
}

func TestMinFunc(t *testing.T) {
	points := createTestPoints()
	result, err := MinFunc(points, []string{"price"}, []string{"min_price"})

	if err != nil {
		t.Errorf("MinFunc returned unexpected error: %v", err)
	}

	minVal, err := result.GetFieldValue("min_price")
	if err != nil || minVal != 10.0 {
		t.Errorf("MinFunc = %v, want 10.0", minVal)
	}
}

func TestVarianceFunc(t *testing.T) {
	points := createTestPoints()
	result, err := VarianceFunc(points, []string{"price"}, []string{"variance_price"})

	if err != nil {
		t.Errorf("VarianceFunc returned unexpected error: %v", err)
	}

	varianceVal, err := result.GetFieldValue("variance_price")
	if err != nil {
		t.Errorf("VarianceFunc returned error getting value: %v", err)
	}

	// Expected variance for [10, 20, 30] is 100
	expectedVariance, err := stats.Variance([]float64{10, 20, 30})
	if err != nil {
		t.Errorf("Error calculating expected variance: %v", err)
	}
	if varianceVal.(float64) < expectedVariance-0.0001 || varianceVal.(float64) > expectedVariance+0.0001 {
		t.Errorf("VarianceFunc = %v, want %v", varianceVal, expectedVariance)
	}
}

func TestStandardDeviationFunc(t *testing.T) {
	points := createTestPoints()
	result, err := StandardDeviationFunc(points, []string{"price"}, []string{"stddev_price"})

	if err != nil {
		t.Errorf("StandardDeviationFunc returned unexpected error: %v", err)
	}

	stdDevVal, err := result.GetFieldValue("stddev_price")
	if err != nil {
		t.Errorf("StandardDeviationFunc returned error getting value: %v", err)
	}

	expectedStdDev, err := stats.StandardDeviationSample([]float64{10, 20, 30})
	if err != nil {
		t.Errorf("Error calculating expected standard deviation: %v", err)
	}
	if stdDevVal.(float64) < expectedStdDev-0.0001 || stdDevVal.(float64) > expectedStdDev+0.0001 {
		t.Errorf("StandardDeviationFunc = %v, want %v", stdDevVal, expectedStdDev)
	}
}

func TestMedianFunc(t *testing.T) {
	points := createTestPoints()
	result, err := MedianFunc(points, []string{"price"}, []string{"median_price"})

	if err != nil {
		t.Errorf("MedianFunc returned unexpected error: %v", err)
	}

	medianVal, err := result.GetFieldValue("median_price")
	if err != nil || medianVal != 20.0 {
		t.Errorf("MedianFunc = %v, want 20.0", medianVal)
	}
}

func TestFloatFieldValToFloat(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		want    float64
		wantErr bool
	}{
		{"float64 input", float64(10.5), 10.5, false},
		{"int input", 10, 10.0, false},
		{"invalid input", "10.5", 0.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := floatFieldValToFloat(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("floatFieldValToFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("floatFieldValToFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateVWAP(t *testing.T) {
	points := createTestPoints()
	result, err := VWAPFunc(points, []string{"price", "volume"}, []string{"vwap"})
	if err != nil {
		t.Errorf("CalculateVWAP returned unexpected error: %v", err)
	}

	vwapVal, err := result.GetFieldValue("vwap")
	if err != nil {
		t.Errorf("CalculateVWAP returned error getting value: %v", err)
	}
	assert.InDelta(t, 23.33333, vwapVal, 0.0001)
}

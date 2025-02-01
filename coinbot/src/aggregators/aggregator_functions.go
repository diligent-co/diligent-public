package aggregators

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/montanaflynn/stats"

	"coinbot/src/datamodels"
	"coinbot/src/utils/errors"
)

type AggregatorFunction func(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error)

func CreateConstantValueAggregatorFunc(inputFieldNames []string, outputFieldNames []string, constantValue float64) AggregatorFunction {
	return func(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
		return &datamodels.GenericDataPoint{
			Timestamp: time.Unix(0, 0), //points[len(points)-1].GetTimestamp(),
			Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: constantValue, FieldType: datamodels.FieldTypeFloat}},
		}, nil
	}
}

func LastValueFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	// the output field names should be the same len as the input field names or empty
	if len(outputFieldNames) != len(inputFieldNames) && len(outputFieldNames) != 0 {
		return nil, fmt.Errorf("last value function requires output field names to be the same length as input field names or empty")
	}
	if len(points) == 0 {
		return nil, nil
	}
	if len(inputFieldNames) == 0 {
		slog.Debug("LastValueFunc called with no fields, using all fields")
		return points[len(points)-1], nil
	}
	// create a new datapoint out of the last values from each field
	lastPoint := points[len(points)-1]
	lastValues := make([]datamodels.DataPointElement, 0)
	for _, field := range inputFieldNames {
		if el, err := lastPoint.Get(field); err == nil {
			lastValues = append(lastValues, el)
		} else {
			return nil, errors.Wrapf(err,
				fmt.Sprintf("field %s not found in data point's fields %s", field, lastPoint.GetFields()))
		}
	}
	return &datamodels.GenericDataPoint{
		Timestamp: lastPoint.GetTimestamp(),
		Elements:  lastValues,
	}, nil
}

func LastValueF1DividedByF2Func(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(inputFieldNames) != 2 {
		return nil, fmt.Errorf("last value f1 divided by f2 function requires exactly two fields")
	}
	f1 := inputFieldNames[0]
	f2 := inputFieldNames[1]
	lastPoint := points[len(points)-1]
	f1Val, err := lastPoint.GetFieldValue(f1)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get f1 '%s'", f1)
	}
	f2Val, err := lastPoint.GetFieldValue(f2)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get f2 '%s'", f2)
	}
	f1Float, err := floatFieldValToFloat(f1Val)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert f1 '%s' to float", f1)
	}
	f2Float, err := floatFieldValToFloat(f2Val)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert f2 '%s' to float", f2)
	}
	return &datamodels.GenericDataPoint{
		Timestamp: lastPoint.GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: f1Float / f2Float, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func LastValueF1MinusF2DividedByF3Func(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(inputFieldNames) != 3 {
		return nil, fmt.Errorf("last value f1 minus f2 divided by f3 function requires exactly three fields")
	}
	f1 := inputFieldNames[0]
	f2 := inputFieldNames[1]
	f3 := inputFieldNames[2]
	lastPoint := points[len(points)-1]
	f1Val, err := lastPoint.GetFieldValue(f1)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get f1 '%s'", f1)
	}
	f2Val, err := lastPoint.GetFieldValue(f2)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get f2 '%s'", f2)
	}
	f3Val, err := lastPoint.GetFieldValue(f3)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get f3 '%s'", f3)
	}
	f1Float, err := floatFieldValToFloat(f1Val)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert f1 '%s' to float", f1)
	}
	f2Float, err := floatFieldValToFloat(f2Val)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert f2 '%s' to float", f2)
	}
	f3Float, err := floatFieldValToFloat(f3Val)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert f3 '%s' to float", f3)
	}
	return &datamodels.GenericDataPoint{
		Timestamp: lastPoint.GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: (f1Float - f2Float) / f3Float, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func MeanFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("no points to calculate mean")
	}
	if len(inputFieldNames) != 1 {
		return nil, fmt.Errorf("mean function requires exactly one field")
	}
	field := inputFieldNames[0]
	// get the average price of the points
	sum := 0.0
	for _, p := range points {
		if val, err := p.GetFieldValue(field); err == nil {
			floatVal, err := floatFieldValToFloat(val)
			if err != nil {
				return &datamodels.GenericDataPoint{}, err
			}
			sum += floatVal
		}
	}
	average := sum / float64(len(points))
	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: average, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func MaxFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("no points to calculate max")
	}
	if len(inputFieldNames) != 1 {
		return nil, fmt.Errorf("max function requires exactly one field")
	}
	field := inputFieldNames[0]
	max := 0.0
	for _, p := range points {
		if val, err := p.GetFieldValue(field); err == nil {
			floatVal, err := floatFieldValToFloat(val)
			if err != nil {
				return &datamodels.GenericDataPoint{}, err
			}
			if floatVal > max {
				max = floatVal
			}
		}
	}
	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: max, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func MinFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	// the output field name should be 0 or 1
	if len(outputFieldNames) != 1 {
		return nil, fmt.Errorf("min function requires exactly one output field")
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("no points to calculate min")
	}
	if len(inputFieldNames) != 1 {
		return nil, fmt.Errorf("min function requires exactly one field")
	}
	field := inputFieldNames[0]
	min := math.MaxFloat64
	for _, p := range points {
		if val, err := p.GetFieldValue(field); err == nil {
			floatVal, err := floatFieldValToFloat(val)
			if err != nil {
				return &datamodels.GenericDataPoint{}, err
			}
			if floatVal < min {
				min = floatVal
			}
		}
	}
	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: min, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func VarianceFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(outputFieldNames) != 1 {
		return nil, fmt.Errorf("variance function requires exactly one output field")
	}
	field := inputFieldNames[0]
	// calculate the variance of the field
	if len(points) < 2 {
		return &datamodels.GenericDataPoint{}, fmt.Errorf("not enough points to calculate variance")
	}
	vals, err := getFloatVals(points, field)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	variance, err := stats.Variance(vals)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: variance, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func StandardDeviationFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(outputFieldNames) != 1 {
		return nil, fmt.Errorf("standard deviation function requires exactly one output field")
	}
	field := inputFieldNames[0]
	// calculate the variance of the field
	if len(points) < 2 {
		return &datamodels.GenericDataPoint{}, fmt.Errorf("not enough points to calculate variance")
	}
	vals, err := getFloatVals(points, field)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	standardDeviation, err := stats.StandardDeviationSample(vals)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: standardDeviation, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func MedianFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(outputFieldNames) != 1 {
		return nil, fmt.Errorf("median function requires exactly one output field")
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("no points to calculate median")
	}
	if len(inputFieldNames) != 1 {
		return nil, fmt.Errorf("median function requires exactly one input field")
	}
	field := inputFieldNames[0]
	vals, err := getFloatVals(points, field)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	median, err := stats.Median(vals)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: median, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func LinRegSlopeFunc(points []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(points) < 2 {
		return nil, fmt.Errorf("not enough points to calculate linear regression slope")
	}
	if len(inputFieldNames) != 1 {
		return nil, fmt.Errorf("linear regression slope function requires exactly one input field")
	}
	field := inputFieldNames[0]
	// x's are (timestamp - start_timestamp)
	// y's are the field values
	XYs := []stats.Coordinate{}
	for _, p := range points {
		xVal := float64(p.GetTimestamp().Unix() - points[0].GetTimestamp().Unix())
		if val, err := p.GetFieldValue(field); err == nil {
			yVal, err := floatFieldValToFloat(val)
			if err != nil {
				return &datamodels.GenericDataPoint{}, err
			}
			XYs = append(XYs, stats.Coordinate{X: xVal, Y: yVal})
		}
	}
	lr, err := stats.LinReg(XYs)
	if err != nil {
		return &datamodels.GenericDataPoint{}, err
	}
	// slope needs to be calculated as rise/run
	// rise is the difference between the last and first y values
	// run is the difference between the last and first x values
	rise := lr[len(lr)-1].Y - lr[0].Y
	run := lr[len(lr)-1].X - lr[0].X
	slope := rise / run

	return &datamodels.GenericDataPoint{
		Timestamp: points[len(points)-1].GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: slope, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func VWAPFunc(dataPoints []datamodels.DataPoint, inputFieldNames []string, outputFieldNames []string) (datamodels.DataPoint, error) {
	if len(inputFieldNames) != 2 {
		return nil, fmt.Errorf("VWAP function requires exactly two input fields (price and volume)")
	}
	slog.Debug("Calculating VWAP", "priceField", inputFieldNames[0], "volumeField", inputFieldNames[1])
	priceField := inputFieldNames[0]
	volumeField := inputFieldNames[1]

	if len(dataPoints) == 0 {
		return nil, fmt.Errorf("no data points to calculate VWAP")
	}

	var volumeTotal float64
	var volumePriceTotal float64

	for _, point := range dataPoints {
		price, priceExists := point.GetValues()[priceField]
		volume, volumeExists := point.GetValues()[volumeField]

		if !priceExists || !volumeExists {
			return nil, fmt.Errorf("price or volume field missing or wrong type")
		}

		priceFloat, err := floatFieldValToFloat(price)
		if err != nil {
			return nil, err
		}
		volumeFloat, err := floatFieldValToFloat(volume)
		if err != nil {
			return nil, err
		}

		volumePriceTotal += priceFloat * volumeFloat
		volumeTotal += volumeFloat
	}

	if volumeTotal == 0 {
		return nil, fmt.Errorf("total volume is zero")
	}

	vwap := volumePriceTotal / volumeTotal

	// Create new data point with the VWAP value
	lastPoint := dataPoints[len(dataPoints)-1]
	return &datamodels.GenericDataPoint{
		Timestamp: lastPoint.GetTimestamp(),
		Elements:  []datamodels.DataPointElement{{Field: outputFieldNames[0], Value: vwap, FieldType: datamodels.FieldTypeFloat}},
	}, nil
}

func getFloatVals(points []datamodels.DataPoint, field string) ([]float64, error) {
	vals := []float64{}
	for _, p := range points {
		if val, err := p.GetFieldValue(field); err == nil {
			floatVal, err := floatFieldValToFloat(val)
			if err != nil {
				return []float64{}, err
			}
			vals = append(vals, floatVal)
		}
	}
	return vals, nil
}

func floatFieldValToFloat(val interface{}) (float64, error) {
	floatVal, ok := val.(float64)
	if !ok {
		intVal, ok := val.(int)
		if !ok {
			return 0, fmt.Errorf("field value is not a float64 or int")
		}
		floatVal = float64(intVal)
	}
	return floatVal, nil
}

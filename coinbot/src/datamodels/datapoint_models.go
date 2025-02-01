package datamodels

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type FieldType string

const (
	FieldTypeFloat  FieldType = "float"
	FieldTypeString FieldType = "string"
	FieldTypeBool   FieldType = "bool"
	FieldTypeInt    FieldType = "int"
	FieldTypeTime   FieldType = "time"
)

func MapFieldType(structField reflect.StructField) FieldType {
	// try to map to a field type
	switch structField.Type.String() {
	case "float32":
		return FieldTypeFloat
	case "float64":
		return FieldTypeFloat
	case "string":
		return FieldTypeString
	case "bool":
		return FieldTypeBool
	case "int":
		return FieldTypeInt
	case "int32":
		return FieldTypeInt
	case "int64":
		return FieldTypeInt
	case "time":
		return FieldTypeTime
	default:
		return FieldType(structField.Type.String())
	}
}

// DataPoint represents a single data point from a feed
type DataPoint interface {
	// GetTimestamp returns the timestamp of the data point
	GetTimestamp() time.Time
	// generic get method
	Get(field string) (DataPointElement, error)
	// GetFieldValue returns the value for the given field name as an interface{}
	GetFieldValue(field string) (interface{}, error)
	// GetFields returns a list of available field names, and their types
	GetFields() map[string]FieldType
	// GetValues returns a map of field names to their values
	GetValues() map[string]interface{}
	// GetFieldType returns the type of a field as a string
	GetFieldType(field string) (FieldType, error)
	// SetFieldValue sets the value for the given field name
	SetFieldValue(field string, value interface{}) error
	// AddElement adds an element to the data point
	AddElement(field string, fieldType FieldType, value interface{})

	String() string
	Copy() DataPoint
}

type DataPointSubscription struct {
	SubscriptionName string
	SubscriptionId   string
	DataPointChan    chan DataPoint
	DoneChan         chan struct{}
	ErrorChan        chan error
}

type DataPointElement struct {
	Field     string
	FieldType FieldType
	Value     interface{}
}

func (g *DataPointElement) String() string {
	return fmt.Sprintf("{Field: %s, FieldType: %s, Value: %v}", g.Field, g.FieldType, g.Value)
}

func (g *DataPointElement) GetTypedValue() (interface{}, error) {
	switch g.FieldType {
	case FieldTypeFloat:
		return g.Value.(float64), nil
	case FieldTypeString:
		return g.Value.(string), nil
	case FieldTypeBool:
		return g.Value.(bool), nil
	case FieldTypeInt:
		return g.Value.(int), nil
	case FieldTypeTime:
		return g.Value.(time.Time), nil
	}
	return nil, fmt.Errorf("unsupported field type: %s", g.FieldType)
}

type GenericDataPoint struct {
	Timestamp time.Time
	Elements  []DataPointElement
	mutex     sync.RWMutex
}

func (g *GenericDataPoint) Get(field string) (DataPointElement, error) {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	for _, element := range g.Elements {
		if element.Field == field {
			return element, nil
		}
	}
	return DataPointElement{}, fmt.Errorf("field %s not found", field)
}

func (g *GenericDataPoint) GetTimestamp() time.Time {
	return g.Timestamp
}

func (g *GenericDataPoint) GetFieldValue(field string) (interface{}, error) {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	for _, element := range g.Elements {
		if element.Field == field {
			return element.Value, nil
		}
	}
	return nil, fmt.Errorf("field %s not found", field)
}

func (g *GenericDataPoint) GetFields() map[string]FieldType {
	fields := make(map[string]FieldType)
	for _, element := range g.Elements {
		fields[element.Field] = element.FieldType
	}
	return fields
}

func (g *GenericDataPoint) GetValues() map[string]interface{} {
	values := make(map[string]interface{})
	for _, element := range g.Elements {
		values[element.Field] = element.Value
	}
	return values
}

func (g *GenericDataPoint) GetFieldType(field string) (FieldType, error) {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	for _, element := range g.Elements {
		if element.Field == field {
			return element.FieldType, nil
		}
	}
	return "", fmt.Errorf("field %s not found", field)
}

func (g *GenericDataPoint) SetFieldValue(field string, value interface{}) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	for i, element := range g.Elements {
		if element.Field == field {
			g.Elements[i].Value = value
			return nil
		}
	}
	return fmt.Errorf("field %s not found", field)
}

func (g *GenericDataPoint) String() string {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	// {Timestamp: timestamp, Elements: [{Field: field, FieldType: fieldType, Value: value}, ...]}
	elementStrings := make([]string, len(g.Elements))
	for i, element := range g.Elements {
		elementStrings[i] = element.String()
	}

	elementString := strings.Join(elementStrings, ", ")

	return fmt.Sprintf("{Timestamp: %s, Elements: [%s]}", g.Timestamp, elementString)
}

func (g *GenericDataPoint) AddElement(field string, fieldType FieldType, value interface{}) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.Elements = append(g.Elements, DataPointElement{Field: field, FieldType: fieldType, Value: value})
}

func (g *GenericDataPoint) Copy() DataPoint {
	// thread safe deep copy
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	copy := &GenericDataPoint{
		Timestamp: g.Timestamp,
		Elements:  make([]DataPointElement, len(g.Elements)),
		mutex:     sync.RWMutex{},
	}
	for i, element := range g.Elements {
		copy.Elements[i] = element
	}
	return copy
}

type CsvField struct {
	ColumnIndex  int
	FieldName    string
	FieldType    FieldType
	DefaultValue interface{}
}

type CsvWithTimestampSchema struct {
	RelationName       string
	TimestampFieldName string
	Fields             []CsvField
}

func (s *CsvWithTimestampSchema) GetFieldIndex(field string) (int, error) {
	for _, f := range s.Fields {
		if f.FieldName == field {
			return f.ColumnIndex, nil
		}
	}
	return -1, fmt.Errorf("field %s not found", field)
}

func (s *CsvWithTimestampSchema) GetTimestampFieldIndex() int {
	for _, field := range s.Fields {
		if field.FieldName == s.TimestampFieldName {
			return field.ColumnIndex
		}
	}
	return -1
}

func (s *CsvWithTimestampSchema) GetField(field string) (CsvField, error) {
	index, err := s.GetFieldIndex(field)
	if err != nil {
		return CsvField{}, err
	}
	return s.Fields[index], nil
}

func (s *CsvWithTimestampSchema) GetFieldByIndex(index int) (CsvField, error) {
	if index < 0 || index >= len(s.Fields) {
		return CsvField{}, fmt.Errorf("index out of bounds: %d", index)
	}
	// find field with column index == index
	for _, field := range s.Fields {
		if field.ColumnIndex == index {
			return field, nil
		}
	}
	return CsvField{}, fmt.Errorf("field with column index %d not found", index)
}

func (s *CsvWithTimestampSchema) GetFieldsOrdered() []CsvField {
	// order fields by column index
	fields := make([]CsvField, len(s.Fields))
	for _, field := range s.Fields {
		fields[field.ColumnIndex] = field
	}
	return fields
}
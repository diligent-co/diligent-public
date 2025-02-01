package datamodels

import (
	"testing"
	"time"
)

func TestGenericDataPoint(t *testing.T) {
	// Setup test data
	now := time.Now()
	gdp := &GenericDataPoint{
		Timestamp: now,
		Elements: []DataPointElement{
			{
				Field:     "price",
				FieldType: FieldTypeFloat,
				Value:     42.5,
			},
			{
				Field:     "symbol",
				FieldType: FieldTypeString,
				Value:     "BTC",
			},
			{
				Field:     "active",
				FieldType: FieldTypeBool,
				Value:     true,
			},
		},
	}

	t.Run("GetTimestamp", func(t *testing.T) {
		if ts := gdp.GetTimestamp(); ts != now {
			t.Errorf("GetTimestamp() = %v, want %v", ts, now)
		}
	})

	t.Run("GetFieldValue", func(t *testing.T) {
		tests := []struct {
			name    string
			field   string
			want    interface{}
			wantErr bool
		}{
			{"existing float field", "price", 42.5, false},
			{"existing string field", "symbol", "BTC", false},
			{"existing bool field", "active", true, false},
			{"non-existent field", "notfound", nil, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := gdp.GetFieldValue(tt.field)
				if (err != nil) != tt.wantErr {
					t.Errorf("GetFieldValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !tt.wantErr && got != tt.want {
					t.Errorf("GetFieldValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("GetFields", func(t *testing.T) {
		fields := gdp.GetFields()
		expected := map[string]FieldType{
			"price":  FieldTypeFloat,
			"symbol": FieldTypeString,
			"active": FieldTypeBool,
		}

		if len(fields) != len(expected) {
			t.Errorf("GetFields() returned %d fields, want %d", len(fields), len(expected))
		}

		for field, fieldType := range expected {
			if got := fields[field]; got != fieldType {
				t.Errorf("GetFields()[%s] = %v, want %v", field, got, fieldType)
			}
		}
	})

	t.Run("GetValues", func(t *testing.T) {
		values := gdp.GetValues()
		expected := map[string]interface{}{
			"price":  42.5,
			"symbol": "BTC",
			"active": true,
		}

		if len(values) != len(expected) {
			t.Errorf("GetValues() returned %d values, want %d", len(values), len(expected))
		}

		for field, value := range expected {
			if got := values[field]; got != value {
				t.Errorf("GetValues()[%s] = %v, want %v", field, got, value)
			}
		}
	})

	t.Run("GetFieldType", func(t *testing.T) {
		tests := []struct {
			name    string
			field   string
			want    FieldType
			wantErr bool
		}{
			{"existing float field", "price", FieldTypeFloat, false},
			{"existing string field", "symbol", FieldTypeString, false},
			{"existing bool field", "active", FieldTypeBool, false},
			{"non-existent field", "notfound", "", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := gdp.GetFieldType(tt.field)
				if (err != nil) != tt.wantErr {
					t.Errorf("GetFieldType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !tt.wantErr && got != tt.want {
					t.Errorf("GetFieldType() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("SetFieldValue", func(t *testing.T) {
		tests := []struct {
			name    string
			field   string
			value   interface{}
			wantErr bool
		}{
			{"update existing field", "price", 43.5, false},
			{"update with different type", "price", "invalid", false}, // Note: Go's interface{} allows this
			{"non-existent field", "notfound", 42.5, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := gdp.SetFieldValue(tt.field, tt.value)
				if (err != nil) != tt.wantErr {
					t.Errorf("SetFieldValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if !tt.wantErr {
					if got, _ := gdp.GetFieldValue(tt.field); got != tt.value {
						t.Errorf("After SetFieldValue(), GetFieldValue() = %v, want %v", got, tt.value)
					}
				}
			})
		}
	})
}

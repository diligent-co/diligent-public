package general

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"strings"
)

func TestGetCurrentFilepath(t *testing.T) {
	path := GetCurrentFilepath()
	if path == "" {
		t.Error("Expected non-empty filepath")
	}
	if !filepath.IsAbs(path) {
		t.Error("Expected absolute path")
	}
}

func TestGetCurrentDir(t *testing.T) {
	dir := GetCurrentDir()
	if dir == "" {
		t.Error("Expected non-empty directory")
	}
	if !filepath.IsAbs(dir) {
		t.Error("Expected absolute path")
	}
}

func TestIsValidURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    bool
		wantMsg string
	}{
		{"Valid HTTP URL", "http://example.com", true, ""},
		{"Valid HTTPS URL", "https://example.com/path", true, ""},
		{"Empty URL", "", false, "URL is empty"},
		{"Missing Scheme", "example.com", false, "URL scheme is missing"},
		{"Invalid URL", "http://", false, "URL host is missing"},
		{"Malformed URL", "http:////", false, "URL host is missing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMsg := IsValidURL(tt.url)
			if got != tt.want {
				t.Errorf("IsValidURL() got = %v, want %v", got, tt.want)
			}
			if gotMsg != tt.wantMsg {
				t.Errorf("IsValidURL() gotMsg = %v, want %v", gotMsg, tt.wantMsg)
			}
		})
	}
}

func TestCopyURLToBucket(t *testing.T) {
	// Create a test server that serves a simple file
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	// Skip if not in integration test environment
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	err := CopyURLToBucket(ctx, server.URL, "test-bucket", "test-object")
	if err != nil {
		t.Errorf("CopyURLToBucket() error = %v", err)
	}
}

func TestItemInSlice(t *testing.T) {
	slice := []string{"apple", "banana", "orange"}

	tests := []struct {
		name     string
		slice    []string
		item     string
		expected bool
	}{
		{"Existing item", slice, "banana", true},
		{"Non-existing item", slice, "grape", false},
		{"Empty slice", []string{}, "apple", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ItemInSlice(tt.slice, tt.item)
			if got != tt.expected {
				t.Errorf("ItemInSlice() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertMixedTypesToFloat64Array(t *testing.T) {
	tests := []struct {
		name    string
		input   []interface{}
		want    []float64
		wantErr bool
	}{
		{
			name:    "All float64",
			input:   []interface{}{1.0, 2.0, 3.0},
			want:    []float64{1.0, 2.0, 3.0},
			wantErr: false,
		},
		{
			name:    "Mixed types valid",
			input:   []interface{}{1.0, "2.0", 3.0},
			want:    []float64{1.0, 2.0, 3.0},
			wantErr: false,
		},
		{
			name:    "Invalid string",
			input:   []interface{}{1.0, "invalid", 3.0},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid type",
			input:   []interface{}{1.0, true, 3.0},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertMixedTypesToFloat64Array(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertMixedTypesToFloat64Array() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("ConvertMixedTypesToFloat64Array() got = %v, want %v", got, tt.want)
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("ConvertMixedTypesToFloat64Array() got[%d] = %v, want[%d] = %v", i, got[i], i, tt.want[i])
					}
				}
			}
		})
	}
}

func TestInspectData(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		contains string
	}{
		{
			name:     "Byte slice",
			input:    []byte("test"),
			contains: "Data is []byte",
		},
		{
			name:     "Map",
			input:    map[string]interface{}{"key": "value"},
			contains: "Data is map[string]interface{}",
		},
		{
			name:     "Interface slice",
			input:    []interface{}{1, "two", 3.0},
			contains: "Data is []interface{}",
		},
		{
			name:     "String",
			input:    "test string",
			contains: "Data is string",
		},
		{
			name:     "Unknown type",
			input:    123,
			contains: "Data is of unknown type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InspectData(tt.input)
			if !strings.Contains(got, tt.contains) {
				t.Errorf("InspectData() = %v, want it to contain %v", got, tt.contains)
			}
		})
	}
}
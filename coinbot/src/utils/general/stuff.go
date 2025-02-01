package general

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

func GetCurrentFilepath() string {
	_, filename, _, _ := runtime.Caller(1)
	return filepath.Dir(filename)
}

func GetCurrentDir() string {
	return filepath.Dir(GetCurrentFilepath())
}

func ListEnvVars() {
	for _, env := range os.Environ() {
		slog.Info("Environment variable", "env", env)
	}
}

func GenerateUUID5StringFromByteArray(p []byte) string {
	UUID5Namespace := "f1c8f8d4-1a8e-4b2c-9d3f-5e6c7b8a9d0c"

	namespaceUUID, err := uuid.Parse(UUID5Namespace)
	if err != nil {
		slog.Warn(fmt.Sprintf("Error parsing namespace UUID: %+v", err))
	}
	// Generate the UUID version 5.
	uuid5 := uuid.NewSHA1(namespaceUUID, p)
	return uuid5.String()
}

// IsValidURL checks if a string is a valid URL with allowed schemes
func IsValidURL(rawURL string) (bool, string) {

	// Trim spaces
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return false, "URL is empty"
	}

	// Parse the URL
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return false, fmt.Sprintf("Invalid URL format: %v", err)
	}

	// Check scheme
	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme == "" {
		return false, "URL scheme is missing"
	}

	// Check host
	if parsedURL.Host == "" {
		return false, "URL host is missing"
	}

	return true, ""
}

func CopyURLToBucket(ctx context.Context, url, bucketName, objectPath string) error {
	// Create storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// Get the file from URL
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the bucket handle
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectPath)

	// Create a new bucket writer
	writer := obj.NewWriter(ctx)

	// Copy the data
	if _, err := io.Copy(writer, resp.Body); err != nil {
		writer.Close()
		return err
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

func ItemInSlice[T comparable](slice []T, item T) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func NoDuplicateItemsInSlice[T comparable](slice []T) bool {
	seen := make(map[T]bool)
	for _, item := range slice {
		if seen[item] {
			return false
		}
		seen[item] = true
	}
	return true
}

func ConvertMixedTypesToFloat64Array(data []interface{}) ([]float64, error) {
	resultArray := make([]float64, 0)

	var err error
	for _, element := range data {
		elementFloat, ok := element.(float64)
		if !ok {
			// try to convert to string
			elementString, ok := element.(string)
			if !ok {
				return nil, fmt.Errorf("invalid element type: %v", element)
			}
			elementFloat, err = strconv.ParseFloat(elementString, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid element type: %v", element)
			}
		}
		resultArray = append(resultArray, elementFloat)
	}

	return resultArray, nil
}

func InspectData(data interface{}) string {
	switch data.(type) {
	case []byte:
		return fmt.Sprintf("Data is []byte: %v", data)
	case map[string]interface{}:
		return fmt.Sprintf("Data is map[string]interface{}: %v", data)
	case []interface{}:
		return fmt.Sprintf("Data is []interface{}: %v", data)
	case string:
		return fmt.Sprintf("Data is string: %v", data)
	default:
		return fmt.Sprintf("Data is of unknown type: %v", data)
	}
}

func ChannelAtLoadLevel[T any](channel <-chan T, loadLevel float64) bool {
	return float64(len(channel))/float64(cap(channel)) >= loadLevel
}

func GetSystemUsage() map[string]string {
	// get memory, cpu usage, etc
	report := make(map[string]string)

	// get memory usage
	numCpu := runtime.NumCPU()
	report["num_cpu"] = fmt.Sprintf("%d", numCpu)

	// go routine count
	numGoroutine := runtime.NumGoroutine()
	report["num_goroutine"] = fmt.Sprintf("%d", numGoroutine)

	memoryUsage := runtime.MemStats{}
	runtime.ReadMemStats(&memoryUsage)
	report["memory_usage"] = fmt.Sprintf("%d", memoryUsage.Alloc)
	report["memory_total"] = fmt.Sprintf("%d", memoryUsage.TotalAlloc)
	report["memory_heap_alloc"] = fmt.Sprintf("%d", memoryUsage.HeapAlloc)
	report["memory_heap_sys"] = fmt.Sprintf("%d", memoryUsage.HeapSys)
	report["memory_heap_idle"] = fmt.Sprintf("%d", memoryUsage.HeapIdle)
	report["memory_heap_inuse"] = fmt.Sprintf("%d", memoryUsage.HeapInuse)
	report["memory_heap_released"] = fmt.Sprintf("%d", memoryUsage.HeapReleased)
	report["memory_heap_objects"] = fmt.Sprintf("%d", memoryUsage.HeapObjects)

	return report
}

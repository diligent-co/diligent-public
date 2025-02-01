package strategies

import (
	"bytes"
	"coinbot/src/datamodels"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type ExternalSignalSupplier struct {
	config     *datamodels.ExternalSupplierConfig
	httpClient *http.Client
	assets     []string
}

func NewExternalSignalSupplier(config *datamodels.ExternalSupplierConfig, assets []string) *ExternalSignalSupplier {
	return &ExternalSignalSupplier{
		config: config,
		httpClient: &http.Client{
			Timeout: time.Duration(config.Timeout),
		},
		assets: assets,
	}
}

func (e *ExternalSignalSupplier) GetInputFields() []string {
	return e.config.InputFields
}

func (e *ExternalSignalSupplier) GetOutputFields() []string {
	return e.config.OutputFields
}

func (e *ExternalSignalSupplier) GetName() string {
	return e.config.Name
}

type ExternalServiceRequest struct {
	Data   map[string][]datamodels.DataPoint `json:"data"`
	Assets []string                          `json:"assets"`
}

type ExternalServiceResponse struct {
	Signal *datamodels.Signal `json:"signal"`
	Error  string             `json:"error,omitempty"`
}

func (e *ExternalSignalSupplier) GetSignalFunc() SignalFunc {
	return func(data map[string][]datamodels.DataPoint) *datamodels.Signal {
		// Prepare request payload
		request := ExternalServiceRequest{
			Data:   data,
			Assets: e.assets,
		}

		// Convert request to JSON
		jsonData, err := json.Marshal(request)
		if err != nil {
			slog.Error("Failed to marshal request data", "error", err)
			return nil
		}

		// Create HTTP request
		url := fmt.Sprintf("http://%s:%d%s", e.config.Host, e.config.Port, e.config.Endpoint)
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			slog.Error("Failed to create HTTP request", "error", err)
			return nil
		}
		req.Header.Set("Content-Type", "application/json")

		// Make request
		resp, err := e.httpClient.Do(req)
		if err != nil {
			slog.Error("Failed to make HTTP request", "error", err)
			return nil
		}
		defer resp.Body.Close()

		// Parse response
		var response ExternalServiceResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			slog.Error("Failed to decode response", "error", err)
			return nil
		}

		if response.Error != "" {
			slog.Error("External service returned error", "error", response.Error)
			return nil
		}

		return response.Signal
	}
}

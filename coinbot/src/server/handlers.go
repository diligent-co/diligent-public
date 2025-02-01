package server

import (
    "encoding/json"
    "log/slog"
	"net/http"

    "github.com/swaggo/http-swagger"
)

// @title Coinbot API
// @version 1.0
// @description API server for Coinbot trading system
// @host localhost:8080
// @BasePath /

// WebSocketMessageType represents the type of WebSocket message
// @Description Type of message being sent over WebSocket connection
type WebSocketMessageType string

// WebSocket message type constants
const (
	// Metrics message type for sending metrics data
	Metrics WebSocketMessageType = "metrics"
	// Command message type for sending commands
	Command WebSocketMessageType = "command" 
)

// WebSocketMessage represents a message sent over WebSocket
// @Description Message structure for WebSocket communication
type WebSocketMessage struct {
	// Type of the WebSocket message (metrics or command)
	// Required: true
	// Enum: metrics, command	
	MessageType WebSocketMessageType `json:"message_type" example:"metrics"`
	// Raw JSON message payload
	// Required: true
	Message []byte `json:"message"`
}

// WebSocketResponse represents a response sent back over WebSocket
// @Description Response structure for WebSocket communication
type WebSocketResponse struct {
	// Whether the operation was successful
	// Required: true
	Success bool `json:"success" example:"true"`
	// Response payload data
	// Required: false
	Data any `json:"data"`
	// Error message if operation failed
	// Required: false
	Error string `json:"error" example:"Failed to process message"`
}

type CommandAction string

// Command action constants
const (
	Start CommandAction = "start"
	Stop CommandAction = "stop"
)

// CommandData represents a command to be sent over WebSocket
// @Description Data structure for command messages
type CommandData struct {
	// Command action to be performed
	// Required: true
	Action CommandAction `json:"action" example:"start"`
}


// RegisterHealthCheck registers the health check endpoint
// @Summary Health check endpoint
// @Description Returns health status of the Coinbot service
// @Tags health
// @Produce plain
// @Success 200 {string} string "Coinbot is healthy"
// @Router /health [get]
func (s *Server) RegisterHealthCheck() {
	s.httpMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Coinbot is healthy"))
	})
}

// RegisterWebSocketHandler registers the WebSocket endpoint
// @Summary WebSocket connection endpoint
// @Description Establishes WebSocket connection for real-time communication
// @Tags websocket
// @Accept json
// @Produce json
// @Success 101 {string} string "Switching protocols to websocket"
// @Router /ws [get]
func (s *Server) RegisterWebSocketHandler() {
	s.httpMux.HandleFunc("/ws", s.handleWebSocket)
}

// RegisterSwagger registers the Swagger documentation endpoint
// @Summary Swagger documentation endpoint
// @Description Serves Swagger API documentation UI and JSON spec
// @Tags docs
// @Accept json
// @Produce json,html
// @Success 200 {string} string "Swagger documentation UI"
// @Router /swagger [get]
func (s *Server) RegisterSwagger() {
	s.httpMux.HandleFunc("/swagger", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))
}

// handleMetrics processes incoming metric messages over WebSocket
// @Description Handles incoming metrics data over WebSocket connection
// @Accept json
// @Produce json
// @Param payload body []byte true "Metrics payload"
// @Success 200 {object} WebSocketResponse
// @Failure 400 {object} WebSocketResponse
func (s *Server) handleMetrics(payload []byte) WebSocketResponse {
    var metricsData interface{} // Define your metrics data structure
    if err := json.Unmarshal(payload, &metricsData); err != nil {
        slog.Error("Failed to unmarshal metrics payload", "error", err)
        return WebSocketResponse{
            Success: false,
            Error:   err.Error(),
        }
    }
    // Process metrics data
    slog.Info("Received metrics: %+v", metricsData)
    return WebSocketResponse{
        Success: true,
        Data:    metricsData,
    }
}

// handleCommand processes incoming command messages over WebSocket
// @Description Handles incoming command data over WebSocket connection
// @Accept json
// @Produce json
// @Param payload body []byte true "Command payload"
// @Success 200 {object} WebSocketResponse
// @Failure 400 {object} WebSocketResponse
func (s *Server) handleCommand(payload []byte) WebSocketResponse {
    var commandData interface{} // Define your command data structure
    if err := json.Unmarshal(payload, &commandData); err != nil {
        slog.Error("Failed to unmarshal command payload", "error", err)
        return WebSocketResponse{
            Success: false,
            Error:   err.Error(),
        }
    }
    // Process command data
    slog.Info("Received command: %+v", commandData)
    return WebSocketResponse{
        Success: true,
        Data:    commandData,
    }
}


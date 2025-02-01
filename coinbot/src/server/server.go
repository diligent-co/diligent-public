package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
	"coinbot/src/utils/errors"
	"github.com/gorilla/websocket"

	"coinbot/src/metrics"
)

type Server struct {
	addr          string
	upgrader      websocket.Upgrader
	httpMux       *http.ServeMux
	metricsWriter *metrics.WebsocketMetricsWriter
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all connections (for development purposes)
			},
		},
		httpMux: http.NewServeMux(),
	}
}

func (s *Server) WithMetricsWriter(metricsWriter *metrics.WebsocketMetricsWriter) *Server {
	s.metricsWriter = metricsWriter
	return s
}

func (s *Server) Start(ctx context.Context) error {
	if s.metricsWriter == nil {
		return errors.New("metrics writer is nil")
	}
	s.RegisterHealthCheck()
	s.RegisterWebSocketHandler()
	s.RegisterSwagger()
	server := &http.Server{
		Addr:    s.addr,
		Handler: s.httpMux,
	}

	go func() {
		<-ctx.Done()
		slog.Info("Shutting down server")
		if err := server.Close(); err != nil {
			slog.Error("Failed to close server", "error", err)
		}
	}()

	slog.Info(fmt.Sprintf("Starting server on %s", s.addr))
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}


	return nil
}


func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade connection", "error", err)
		return
	}
	defer conn.Close()

	s.metricsWriter.AddClient(conn)
	defer s.metricsWriter.RemoveClient(conn)

	slog.Info("Client connected")

	// welcome message
	welcomeMessage := WebSocketResponse{
		Success: true,
		Data:    "Welcome to the Coinbot WebSocket server",
	}
	if err := conn.WriteJSON(welcomeMessage); err != nil {
		slog.Error("Failed to send welcome message", "error", err)
		return
	}

	for {
		// Read message from the client
		mType, msg, err := conn.ReadMessage()
		if err != nil {
			slog.Error("Error reading message:", "error", err)
			break
		}
		switch mType {
		case websocket.TextMessage:
			slog.Info(fmt.Sprintf("Received text message: %s", msg))
		case websocket.BinaryMessage:
			slog.Info(fmt.Sprintf("Received binary message: %s", msg))
		case websocket.CloseMessage:
			slog.Info("Received close message")
		case websocket.PingMessage:
			slog.Info("Received ping message")
		case websocket.PongMessage:
			slog.Info("Received pong message")
		default:
			slog.Info(fmt.Sprintf("Received unknown message type: %d", mType))
		}

		var wsMessage WebSocketMessage
		if err := json.Unmarshal(msg, &wsMessage); err != nil {
			slog.Error("Failed to unmarshal message", "error", err)
			continue
		}

		switch wsMessage.MessageType {
		case Metrics:
			slog.Info("Server websocket received metrics message")
			metricsResponse := s.handleMetrics(wsMessage.Message)
			if err := conn.WriteJSON(metricsResponse); err != nil {
				slog.Error("Failed to send metrics response", "error", err)
				return
			}
		case Command:
			slog.Info("Server websocket received command message")
			commandResponse := s.handleCommand(wsMessage.Message)
			if err := conn.WriteJSON(commandResponse); err != nil {
				slog.Error("Failed to send command response", "error", err)
				return
			}
		default:
			slog.Info(fmt.Sprintf("Received unknown message type: %s", wsMessage.MessageType))
		}
	}
}

func StartHeartbeat(ctx context.Context) {
	seconds := 10
	timer := time.NewTicker(time.Second * time.Duration(seconds))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutting down heartbeat")
			return
		case <-timer.C:
			slog.Info(fmt.Sprintf("%d second heartbeat", seconds))
		}
	}
}

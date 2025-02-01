package metrics

import (
	"context"
	"sync"

	"github.com/gorilla/websocket"
	"coinbot/src/datamodels"
)

type WebsocketMetricsWriter struct {
	clients   map[*websocket.Conn]bool // this doesn't have to be tied to websockets
	broadcast chan interface{}
	mu        sync.Mutex
}

// NewWebSocketMetricsWriter creates a new WebSocketMetricsWriter
func NewWebSocketMetricsWriter() *WebsocketMetricsWriter {
	return &WebsocketMetricsWriter{
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan interface{}),
	}
}

// AddClient adds a new client connection
func (w *WebsocketMetricsWriter) AddClient(conn *websocket.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.clients[conn] = true
}

// RemoveClient removes a client connection
func (w *WebsocketMetricsWriter) RemoveClient(conn *websocket.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.clients, conn)
}

// sendMetricsToClients sends metrics data to all connected clients
func (w *WebsocketMetricsWriter) Write(ctx context.Context, metrics datamodels.Metric) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for client := range w.clients {
		err := client.WriteJSON(metrics)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WebsocketMetricsWriter) Close() error {
	// Close all clients
	for client := range w.clients {
		client.Close()
	}
	return nil
}

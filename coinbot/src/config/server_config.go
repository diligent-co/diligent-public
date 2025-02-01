package config

import (
	"net/http"

	"github.com/gorilla/websocket"

	"coinbot/src/datamodels"
)


func NewDefaultWSConfig() datamodels.WSConfig {
	return datamodels.WSConfig{
		Upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Configure appropriately for production
			},
		},
	}
}
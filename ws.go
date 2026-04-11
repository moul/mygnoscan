package main

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// wsProxyHandler proxies WebSocket connections to the tx-indexer.
// Browsers can't connect directly because the CDN blocks WS upgrades.
func wsProxyHandler(indexerURL string) http.HandlerFunc {
	// Convert HTTP URL to WebSocket URL
	wsURL := strings.Replace(indexerURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

	return func(w http.ResponseWriter, r *http.Request) {
		// Upgrade browser connection
		clientConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("ws proxy: upgrade failed: %v", err)
			return
		}
		defer clientConn.Close()

		// Connect to indexer
		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			Subprotocols:     []string{"graphql-transport-ws"},
		}
		indexerConn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("ws proxy: indexer dial failed: %v", err)
			clientConn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "indexer unavailable"))
			return
		}
		defer indexerConn.Close()

		// Bidirectional relay
		done := make(chan struct{})

		// Client → Indexer
		go func() {
			defer close(done)
			for {
				msgType, msg, err := clientConn.ReadMessage()
				if err != nil {
					return
				}
				if err := indexerConn.WriteMessage(msgType, msg); err != nil {
					return
				}
			}
		}()

		// Indexer → Client
		for {
			msgType, msg, err := indexerConn.ReadMessage()
			if err != nil {
				return
			}
			if err := clientConn.WriteMessage(msgType, msg); err != nil {
				return
			}
		}
	}
}

// internal/pipeline/pipeline_test.go
// @tag test, pipeline
package pipeline_test

import (
	"CryptoFlow/internal/config"
	"CryptoFlow/internal/model"
	"CryptoFlow/internal/pipeline"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestStartReaders_SpotSnapshotREST tests the REST API fetching and processing for spot snapshots.
func TestStartReaders_SpotSnapshotREST(t *testing.T) {
	// Mock HTTP server for Binance Spot Snapshot
	mockData := `{
        "lastUpdateId": 123456,
        "bids": [["100.1", "1.0"], ["99.8", "2.0"]],
        "asks": [["101.0", "1.5"], ["102.0", "2.5"]]
    }`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, mockData)
	}))
	defer server.Close()

	cfg := &config.Config{
		Exchanges: []*config.ExchangeConfig{
			{
				Name: "binance",
				Spot: &config.SpotConfig{
					OrderbookSnapshots: &config.DataSource{
						Enabled:    true,
						Connection: "rest",
						URL:        server.URL,
						Limit:      10,
						Interval:   100,
						Symbols:    []string{"BTCUSDT"},
					},
				},
			},
		},
	}

	flattenedOutput := make(chan interface{}, 10)
	var wg sync.WaitGroup

	pipeline.StartReaders(cfg, flattenedOutput, &wg)

	select {
	case msg := <-flattenedOutput:
		_, ok := msg.(*model.SpotSnapshotFlattenedRow)
		if !ok {
			t.Fatalf("Expected *model.SpotSnapshotFlattenedRow, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for flattened data from REST fetcher")
	}

	// This is a simple test, for a real-world scenario you'd need a more robust way to
	// terminate the readers or use a context.
	close(flattenedOutput)
	wg.Wait()
}

// TestStartReaders_SpotDeltaWebSocket tests the WebSocket client for spot deltas.
func TestStartReaders_SpotDeltaWebSocket(t *testing.T) {
	// Mock WebSocket server
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Failed to upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		// Wait for the subscription message
		_, subMsg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("Failed to read subscription message: %v", err)
		}
		if !strings.Contains(string(subMsg), "SUBSCRIBE") {
			t.Errorf("Expected SUBSCRIBE message, got: %s", string(subMsg))
		}

		// Send a mock delta message
		mockDelta := `{
            "e": "depthUpdate",
            "E": 1629876543210,
            "s": "BTCUSDT",
            "U": 1,
            "u": 5,
            "b": [["100.0", "0.5"]],
            "a": [["101.0", "0.8"]]
        }`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(mockDelta)); err != nil {
			t.Fatalf("Failed to write mock delta message: %v", err)
		}
	}))
	defer wsServer.Close()

	// Replace ws:// with http:// for httptest
	wsURL := "ws" + strings.TrimPrefix(wsServer.URL, "http")

	cfg := &config.Config{
		Exchanges: []*config.ExchangeConfig{
			{
				Name: "binance",
				Spot: &config.SpotConfig{
					OrderbookDelta: &config.DataSource{
						Enabled: true,
						URL:     wsURL, // Use mock server URL
						Symbols: []string{"BTCUSDT"},
					},
				},
			},
		},
	}

	flattenedOutput := make(chan interface{}, 10)
	var wg sync.WaitGroup

	// Start the readers
	pipeline.StartReaders(cfg, flattenedOutput, &wg)

	select {
	case msg := <-flattenedOutput:
		// Assert the type of the received message
		_, ok := msg.(*model.SpotDeltaFlattenedRow)
		if !ok {
			t.Fatalf("Expected *model.SpotDeltaFlattenedRow, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for flattened data from WebSocket fetcher")
	}

	close(flattenedOutput)
	wg.Wait()
}

// TestStartReaders_FutureSnapshotREST tests the REST API for future snapshots.
func TestStartReaders_FutureSnapshotREST(t *testing.T) {
	// Mock HTTP server for Bybit Future Snapshot
	mockData := `{
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "s": "BTCUSDT",
            "b": [["50000.1", "1.0"]],
            "a": [["50000.5", "1.5"]],
            "ts": 1629876543210,
            "u": 12345
        },
        "time": 1629876543210
    }`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, mockData)
	}))
	defer server.Close()

	cfg := &config.Config{
		Exchanges: []*config.ExchangeConfig{
			{
				Name: "bybit",
				Future: &config.FutureConfig{
					OrderbookSnapshots: &config.DataSource{
						Enabled:    true,
						Connection: "rest",
						URL:        server.URL,
						Limit:      10,
						Interval:   100,
						Symbols:    []string{"BTCUSDT"},
					},
				},
			},
		},
	}

	flattenedOutput := make(chan interface{}, 10)
	var wg sync.WaitGroup

	pipeline.StartReaders(cfg, flattenedOutput, &wg)

	select {
	case msg := <-flattenedOutput:
		_, ok := msg.(*model.FutureSnapshotFlattenedRow)
		if !ok {
			t.Fatalf("Expected *model.FutureSnapshotFlattenedRow, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for flattened data from REST fetcher")
	}

	close(flattenedOutput)
	wg.Wait()
}

// TestStartReaders_FutureDeltaWebSocket tests the WebSocket client for future deltas.
func TestStartReaders_FutureDeltaWebSocket(t *testing.T) {
	// Mock WebSocket server
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Failed to upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		// Wait for the subscription message
		_, subMsg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("Failed to read subscription message: %v", err)
		}
		if !strings.Contains(string(subMsg), "subscribe") {
			t.Errorf("Expected subscribe message, got: %s", string(subMsg))
		}

		// Send a mock delta message
		mockDelta := `{
            "topic": "orderbook.1.BTCUSDT",
            "type": "delta",
            "ts": 1629876543210,
            "data": {
                "b": [["50000.1", "0.5"]],
                "a": [["50000.5", "0.8"]]
            }
        }`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(mockDelta)); err != nil {
			t.Fatalf("Failed to write mock delta message: %v", err)
		}
	}))
	defer wsServer.Close()

	wsURL := "ws" + strings.TrimPrefix(wsServer.URL, "http")

	cfg := &config.Config{
		Exchanges: []*config.ExchangeConfig{
			{
				Name: "bybit",
				Future: &config.FutureConfig{
					OrderbookDelta: &config.DataSource{
						Enabled: true,
						URL:     wsURL,
						Symbols: []string{"BTCUSDT"},
					},
				},
			},
		},
	}

	flattenedOutput := make(chan interface{}, 10)
	var wg sync.WaitGroup

	pipeline.StartReaders(cfg, flattenedOutput, &wg)

	select {
	case msg := <-flattenedOutput:
		_, ok := msg.(*model.FutureDeltaFlattenedRow)
		if !ok {
			t.Fatalf("Expected *model.FutureDeltaFlattenedRow, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for flattened data from WebSocket fetcher")
	}

	close(flattenedOutput)
	wg.Wait()
}

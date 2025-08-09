package models

import (
	"time"
)

// RawOrderbookMessage represents the raw message from exchange websocket
type RawOrderbookMessage struct {
	Exchange    string
	Symbol      string
	Market      string
	Data        []byte
	Timestamp   time.Time
	MessageType string
}

// OrderbookEntry represents a single price level in the orderbook
type OrderbookEntry struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

// OrderbookSnapshot represents the complete orderbook state
type OrderbookSnapshot struct {
	Exchange     string           `json:"exchange"`
	Symbol       string           `json:"symbol"`
	Bids         []OrderbookEntry `json:"bids"`
	Asks         []OrderbookEntry `json:"asks"`
	LastUpdateID int64            `json:"lastUpdateId"`
	Timestamp    time.Time        `json:"timestamp"`
}

// FlattenedOrderbookEntry represents a single flattened orderbook entry
type FlattenedOrderbookEntry struct {
	Exchange     string  `json:"exchange"`
	Symbol       string  `json:"symbol"`
	Market       string  `json:"market"`
	Timestamp    int64   `json:"timestamp"`
	LastUpdateID int64   `json:"last_update_id"`
	Side         string  `json:"side"` // "bid" or "ask"
	Price        float64 `json:"price"`
	Quantity     float64 `json:"quantity"`
	Level        int     `json:"level"` // 1 = best, 2 = second best, etc.
}

// FlattenedOrderbookBatch represents a batch of flattened orderbook entries
type FlattenedOrderbookBatch struct {
	BatchID     string                    `json:"batch_id"`
	Exchange    string                    `json:"exchange"`
	Symbol      string                    `json:"symbol"`
	Market      string                    `json:"market"`
	Entries     []FlattenedOrderbookEntry `json:"entries"`
	RecordCount int                       `json:"record_count"`
	Timestamp   time.Time                 `json:"timestamp"`
	ProcessedAt time.Time                 `json:"processed_at"`
}

// BinanceOrderbookResponse represents the response from Binance API
type BinanceOrderbookResponse struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// BinanceWebSocketMessage represents a websocket message from Binance
type BinanceWebSocketMessage struct {
	Stream string      `json:"stream"`
	Data   interface{} `json:"data"`
}

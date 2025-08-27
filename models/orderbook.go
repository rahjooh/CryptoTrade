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
	Exchange     string    `json:"exchange"`
	Symbol       string    `json:"symbol"`
	Market       string    `json:"market"`
	Timestamp    time.Time `json:"timestamp"`
	LastUpdateID int64     `json:"last_update_id"`
	Side         string    `json:"side"` // "bid" or "ask"
	Price        float64   `json:"price"`
	Quantity     float64   `json:"quantity"`
	Level        int       `json:"level"` // 1 = best, 2 = second best, etc.
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

// RawOrderbookDelta represents a raw order book delta message
type RawOrderbookDelta struct {
	Exchange  string
	Symbol    string
	Market    string
	Data      []byte
	Timestamp time.Time
}

// OrderbookDeltaEntry represents a single order book delta entry
type OrderbookDeltaEntry struct {
	Exchange        string  `json:"exchange"`
	Symbol          string  `json:"symbol"`
	Market          string  `json:"market"`
	EventType       string  `json:"event_type"`
	EventTime       int64   `json:"event_time"`
	TransactionTime int64   `json:"transaction_time"`
	UpdateID        int64   `json:"update_id"`
	PrevUpdateID    int64   `json:"prev_update_id"`
	FirstUpdateID   int64   `json:"first_update_id"`
	Side            string  `json:"side"`
	Price           float64 `json:"price"`
	Quantity        float64 `json:"quantity"`
	ReceivedTime    int64   `json:"received_time"`
}

// OrderbookDeltaBatch represents a batch of order book delta entries
type OrderbookDeltaBatch struct {
	BatchID     string                `json:"batch_id"`
	Exchange    string                `json:"exchange"`
	Symbol      string                `json:"symbol"`
	Market      string                `json:"market"`
	Entries     []OrderbookDeltaEntry `json:"entries"`
	RecordCount int                   `json:"record_count"`
	Timestamp   time.Time             `json:"timestamp"`
	ProcessedAt time.Time             `json:"processed_at"`
}

// BinanceDepthEvent mirrors Binance's depth websocket event structure
type BinanceDepthEvent struct {
	Event            string         `json:"e"`
	Time             int64          `json:"E"`
	TransactionTime  int64          `json:"T"`
	Symbol           string         `json:"s"`
	FirstUpdateID    int64          `json:"U"`
	LastUpdateID     int64          `json:"u"`
	PrevLastUpdateID int64          `json:"pu"`
	Bids             []BinanceLevel `json:"b"`
	Asks             []BinanceLevel `json:"a"`
}

// BinanceLevel represents a single price level in Binance depth events.
type BinanceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

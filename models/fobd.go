package models

import "time"

//// BinanceWebSocketMessage represents a websocket message from Binance
//type BinanceWebSocketMessage struct {
//	Stream string      `json:"stream"`
//	Data   interface{} `json:"data"`
//}

// BinanceLevel represents a single price level in Binance depth events.
type BinanceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
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

// RawFOBDmodel represents a raw order book delta message
type RawFOBDmodel struct {
	Exchange  string
	Symbol    string
	Market    string
	Data      []byte
	Timestamp time.Time
}

// RawFOBDentryModel represents a single order book delta entry
type RawFOBDentryModel struct {
	Symbol        string  `json:"symbol"`
	EventTime     int64   `json:"event_time"`
	UpdateID      int64   `json:"update_id"`
	PrevUpdateID  int64   `json:"prev_update_id"`
	FirstUpdateID int64   `json:"first_update_id"`
	Side          string  `json:"side"`
	Price         float64 `json:"price"`
	Quantity      float64 `json:"quantity"`
	ReceivedTime  int64   `json:"received_time"`
}

// RawFOBDbatchModel represents a batch of order book delta entries
type RawFOBDbatchModel struct {
	BatchID     string              `json:"batch_id"`
	Exchange    string              `json:"exchange"`
	Symbol      string              `json:"symbol"`
	Market      string              `json:"market"`
	Entries     []RawFOBDentryModel `json:"entries"`
	RecordCount int                 `json:"record_count"`
	Timestamp   time.Time           `json:"timestamp"`
	ProcessedAt time.Time           `json:"processed_at"`
}

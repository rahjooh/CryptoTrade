package models

import (
	"encoding/json"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// GENERAL ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// FOBSEntry represents a single price level in the orderbook
type FOBSEntry struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

//// FOBSresp represents the complete orderbook state
//type FOBSresp struct {
//	Exchange     string      `json:"exchange"`
//	Symbol       string      `json:"symbol"`
//	Bids         []FOBSEntry `json:"bids"`
//	Asks         []FOBSEntry `json:"asks"`
//	LastUpdateID int64       `json:"lastUpdateId"`
//	Timestamp    time.Time   `json:"timestamp"`
//}

// RawFOBSMessage represents the raw message from exchange websocket
type RawFOBSMessage struct {
	Exchange    string
	Symbol      string
	Market      string
	Data        []byte
	Timestamp   time.Time
	MessageType string
}

// NormFOBSMessage represents a single flattened orderbook entry
type NormFOBSMessage struct {
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

// BatchFOBSMessage represents a batch of flattened orderbook entries
type BatchFOBSMessage struct {
	BatchID     string            `json:"batch_id"`
	Exchange    string            `json:"exchange"`
	Symbol      string            `json:"symbol"`
	Market      string            `json:"market"`
	Entries     []NormFOBSMessage `json:"entries"`
	RecordCount int               `json:"record_count"`
	Timestamp   time.Time         `json:"timestamp"`
	ProcessedAt time.Time         `json:"processed_at"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BINANCE ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BinanceFOBSresp represents the response from Binance API
type BinanceFOBSresp struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BYBIT /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BybitFOBSresp represents the order book snapshot response from Bybit
type BybitFOBSresp struct {
	Bids     [][]string `json:"b"`
	Asks     [][]string `json:"a"`
	UpdateID int64      `json:"u"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// KUCOIN ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

type OKXRawFOBSMessage struct {
	Exchange   string    `json:"exchange"`
	Market     string    `json:"market"` // e.g. "swap"
	Symbol     string    `json:"symbol"` // e.g. "BTC-USDT-SWAP"
	Depth      int       `json:"depth"`
	FetchedAt  time.Time `json:"fetched_at"`
	ReceivedAt time.Time `json:"received_at"`
	// Payload is the exact OKX REST response body for /market/books, marshaled to JSON bytes.
	Payload json.RawMessage `json:"payload"`
}

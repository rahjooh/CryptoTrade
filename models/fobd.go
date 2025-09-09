package models

import "time"

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// GENERAL ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// FOBDEntry represents a single price level in Binance depth events.
type FOBDEntry struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// RawFOBDMessage represents a raw order book delta message. Wraps raw order-book delta messages from any exchange
type RawFOBDMessage struct {
	Exchange  string
	Symbol    string
	Market    string
	Data      []byte
	Timestamp time.Time
}

// NormFOBDMessage represents a single order book delta entry
type NormFOBDMessage struct {
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

// BatchFOBDMessage represents a batch of order book delta entries
type BatchFOBDMessage struct {
	BatchID     string            `json:"batch_id"`
	Exchange    string            `json:"exchange"`
	Symbol      string            `json:"symbol"`
	Market      string            `json:"market"`
	Entries     []NormFOBDMessage `json:"entries"`
	RecordCount int               `json:"record_count"`
	Timestamp   time.Time         `json:"timestamp"`
	ProcessedAt time.Time         `json:"processed_at"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BINANCE ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

//// BinanceWebSocketMessage represents a websocket message from Binance
//type BinanceWebSocketMessage struct {
//	Stream string      `json:"stream"`
//	Data   interface{} `json:"data"`
//}

// BinanceFOBDResp mirrors Binance's depth websocket event structure
type BinanceFOBDResp struct {
	Event            string      `json:"e"`
	Time             int64       `json:"E"`
	TransactionTime  int64       `json:"T"`
	Symbol           string      `json:"s"`
	FirstUpdateID    int64       `json:"U"`
	LastUpdateID     int64       `json:"u"`
	PrevLastUpdateID int64       `json:"pu"`
	Bids             []FOBDEntry `json:"b"`
	Asks             []FOBDEntry `json:"a"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BYBIT /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BybitFOBDResp represents an order book delta update from Bybit websocket
type BybitFOBDResp struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  struct {
		Symbol   string     `json:"s"`
		Bids     [][]string `json:"b"`
		Asks     [][]string `json:"a"`
		UpdateID int64      `json:"u"`
		Seq      int64      `json:"seq"`
	} `json:"data"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// KUCOIN ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// KucoinFOBDResp represents a level2 delta update from KuCoin futures
// WebSocket. The event contains a sequence identifier, timestamp and bid/ask
// changes represented as arrays of price and quantity strings.
type KucoinFOBDResp struct {
	Symbol    string      `json:"symbol"`
	Sequence  int64       `json:"sequence"`
	Timestamp int64       `json:"timestamp"`
	Bids      []FOBDEntry `json:"bids"`
	Asks      []FOBDEntry `json:"asks"`
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////////////// OKX //////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// OkxFOBDResp represents an order book delta update from OKX websocket.
// The update includes bids and asks arrays alongside an action type and timestamp.
type OkxFOBDResp struct {
	Symbol    string      `json:"symbol"`
	Action    string      `json:"action"`
	Timestamp int64       `json:"timestamp"`
	Bids      []FOBDEntry `json:"bids"`
	Asks      []FOBDEntry `json:"asks"`
}

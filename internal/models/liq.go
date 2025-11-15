package models

import "time"

// RawLiquidationMessage represents a raw liquidation payload captured from an
// exchange specific stream. It keeps the raw JSON payload together with
// metadata that allows downstream writers to route the event appropriately.
type RawLiquidationMessage struct {
	Exchange  string
	Symbol    string
	Market    string
	Data      []byte
	Timestamp time.Time
}

// NormLiquidationMessage is a normalized liquidation entry across exchanges.
type NormLiquidationMessage struct {
	Exchange    string  `json:"exchange"`
	Symbol      string  `json:"symbol"`
	Market      string  `json:"market"`
	Side        string  `json:"side"`
	OrderType   string  `json:"order_type"`
	Price       float64 `json:"price"`
	Quantity    float64 `json:"quantity"`
	EventTime   int64   `json:"event_time"`
	ReceivedTime int64  `json:"received_time"`
	RawPayload  []byte  `json:"-"`
}

// BatchLiquidationMessage groups normalized liquidation rows for writing.
type BatchLiquidationMessage struct {
	BatchID     string                    `json:"batch_id"`
	Exchange    string                    `json:"exchange"`
	Symbol      string                    `json:"symbol"`
	Market      string                    `json:"market"`
	Entries     []NormLiquidationMessage  `json:"entries"`
	RecordCount int                       `json:"record_count"`
	Timestamp   time.Time                 `json:"timestamp"`
	ProcessedAt time.Time                 `json:"processed_at"`
}

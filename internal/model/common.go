// internal/model/common.go
// @tag models, data_structure, core
package model

// ───────────────────────────────────────────────────────────────
// 🚀 Core Data Structures
// ───────────────────────────────────────────────────────────────

// ConnectionType represents the type of connection to an exchange.
type ConnectionType string

const (
	Rest      ConnectionType = "rest"
	Websocket ConnectionType = "websocket"
)

// MarketType defines the type of market (e.g., spot, future).
type MarketType string

const (
	MarketTypeSpot   MarketType = "spot"
	MarketTypeFuture MarketType = "future"
)

// DataSourceType defines the type of data being collected (e.g., orderbook snapshot, delta).
type DataSourceType string

const (
	DataSourceOrderbookSnapshot DataSourceType = "orderbook_snapshot"
	DataSourceOrderbookDelta    DataSourceType = "orderbook_delta"
	DataSourceContracts         DataSourceType = "contracts"
	DataSourceLiquidation       DataSourceType = "liquidation"
	DataSourceOpenInterest      DataSourceType = "open_interest"
	DataSourceFundingRate       DataSourceType = "funding_rate"
)

// OrderBookEntry represents a single price level in an order book.
// This is a fundamental type used in both snapshots and deltas.
type OrderBookEntry struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

// RawData is a container for raw data fetched from an exchange.
type RawData struct {
	Exchange   string
	MarketType MarketType
	DataType   DataSourceType
	Symbol     string
	Data       []byte
}

package model

// Liquidation captures data about a liquidation event on the exchange.
type Liquidation struct {
	Exchange    string  `json:"exchange"`
	MarketType  string  `json:"market_type"`
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"` // buy/sell
	Price       float64 `json:"price"`
	Quantity    float64 `json:"quantity"`
	TimestampMs int64   `json:"timestamp_ms"`
}

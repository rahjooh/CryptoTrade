package model

// FundingRate represents funding rate data for a futures instrument.
type FundingRate struct {
	Exchange    string  `json:"exchange"`
	MarketType  string  `json:"market_type"`
	Symbol      string  `json:"symbol"`
	Rate        float64 `json:"rate"`
	TimestampMs int64   `json:"timestamp_ms"`
}

package models

import (
	"encoding/json"
	"time"
)

// RawFPI carries a single premium-index observation before normalisation.
// Each exchange reader populates this structure before writing it to fpi.raw.
type RawFPI struct {
	Exchange             string          `json:"exchange"`
	Market               string          `json:"market"`
	Symbol               string          `json:"symbol"`
	MarkPrice            float64         `json:"mark_price"`
	IndexPrice           float64         `json:"index_price"`
	EstimatedSettlePrice float64         `json:"estimated_settle_price"`
	FundingRate          float64         `json:"funding_rate"`
	NextFundingTime      time.Time       `json:"next_funding_time"`
	PremiumIndex         float64         `json:"premium_index"`
	EventTime            time.Time       `json:"event_time"`
	Source               string          `json:"source"`
	Payload              json.RawMessage `json:"payload"`
}

// NormFPI is the flattened premium-index representation emitted on fpi.norm.
type NormFPI struct {
	Exchange             string  `json:"exchange"`
	Market               string  `json:"market"`
	Symbol               string  `json:"symbol"`
	EventTimeMs          int64   `json:"event_time_ms"`
	MarkPrice            float64 `json:"mark_price"`
	IndexPrice           float64 `json:"index_price"`
	EstimatedSettlePrice float64 `json:"estimated_settle_price"`
	FundingRate          float64 `json:"funding_rate"`
	NextFundingTimeMs    int64   `json:"next_funding_time_ms"`
	PremiumIndex         float64 `json:"premium_index"`
	ReceivedTimeMs       int64   `json:"received_time_ms"`
	Source               string  `json:"source"`
}

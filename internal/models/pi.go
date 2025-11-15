package models

import "time"

// RawPIMessage holds a single premium-index observation from any exchange.
type RawPIMessage struct {
	Exchange             string
	Symbol               string
	Market               string
	MarkPrice            float64
	IndexPrice           float64
	EstimatedSettlePrice float64
	FundingRate          float64
	NextFundingTime      time.Time
	PremiumIndex         float64
	Source               string
	Timestamp            time.Time
	Payload              []byte
}

// NormPIMessage captures a normalized premium-index sample.
type NormPIMessage struct {
	Symbol               string  `json:"symbol"`
	EventTime            int64   `json:"event_time"`
	MarkPrice            float64 `json:"mark_price"`
	IndexPrice           float64 `json:"index_price"`
	EstimatedSettlePrice float64 `json:"estimated_settle_price"`
	FundingRate          float64 `json:"funding_rate"`
	NextFundingTime      int64   `json:"next_funding_time"`
	PremiumIndex         float64 `json:"premium_index"`
	ReceivedTime         int64   `json:"received_time"`
}

// BatchPIMessage batches normalized premium-index rows for writing.
type BatchPIMessage struct {
	BatchID     string          `json:"batch_id"`
	Exchange    string          `json:"exchange"`
	Symbol      string          `json:"symbol"`
	Market      string          `json:"market"`
	Entries     []NormPIMessage `json:"entries"`
	RecordCount int             `json:"record_count"`
	Timestamp   time.Time       `json:"timestamp"`
	ProcessedAt time.Time       `json:"processed_at"`
}

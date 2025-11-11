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

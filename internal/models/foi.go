package models

import (
	"time"
)

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// GENERAL ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// RawFOIMessage represents the raw open-interest payload from an exchange stream or REST.
type RawFOIMessage struct {
	Exchange  string
	Symbol    string
	Market    string // e.g. "future-openinterest"
	Data      []byte
	Timestamp time.Time
}

// NormFOIMessage is a normalized open-interest reading at a point in time.
type NormFOIMessage struct {
	Symbol       string  `json:"symbol"`
	EventTime    int64   `json:"event_time"`    // ms
	OpenInterest float64 `json:"open_interest"` // in contracts (Binance returns lots/qty)
	ReceivedTime int64   `json:"received_time"` // ms
}

// BatchFOIMessage groups multiple normalized FOI rows.
type BatchFOIMessage struct {
	BatchID     string           `json:"batch_id"`
	Exchange    string           `json:"exchange"`
	Symbol      string           `json:"symbol"`
	Market      string           `json:"market"`
	Entries     []NormFOIMessage `json:"entries"`
	RecordCount int              `json:"record_count"`
	Timestamp   time.Time        `json:"timestamp"`
	ProcessedAt time.Time        `json:"processed_at"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BINANCE ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BinanceFOICurrentResp (USDⓈ-M / COIN-M) current OI (e.g., /fapi/v1/openInterest)
type BinanceFOICurrentResp struct {
	Symbol       string `json:"symbol"`
	OpenInterest string `json:"openInterest"`
	Time         int64  `json:"time"` // event/response ts (ms)
}

// BinanceFOIHistResp (e.g., /futures/data/openInterestHist)
type BinanceFOIHistResp struct {
	Symbol               string `json:"symbol"`
	SumOpenInterest      string `json:"sumOpenInterest"`
	SumOpenInterestValue string `json:"sumOpenInterestValue"`
	Timestamp            int64  `json:"timestamp"` // ms
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BYBIT /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BybitFOIResp (e.g., /derivatives/v3/public/open-interest)
type BybitFOIResp struct {
	Symbol       string `json:"symbol"`
	OpenInterest string `json:"openInterest"`
	Ts           int64  `json:"ts"` // ms
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////////////// OKX //////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// OKXFOIResp (e.g., /api/v5/public/open-interest)
type OKXFOIResp struct {
	InstID string `json:"instId"`
	OI     string `json:"oi"`    // contracts
	OICcy  string `json:"oiCcy"` // coin-denominated OI, if present
	Ts     string `json:"ts"`    // ms as string
}

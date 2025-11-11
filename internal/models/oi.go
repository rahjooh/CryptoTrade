package models

import "time"

// RawOIMessage represents a single open interest observation captured from an
// exchange specific stream. The structure keeps both the parsed metrics and the
// original payload so downstream writers can persist rich context.
type RawOIMessage struct {
	Exchange string
	Symbol   string
	Market   string

	// Value represents the total number of outstanding contracts or
	// positions on the exchange specific units (e.g. contracts or coins).
	Value float64
	// ValueUSD captures the exchange provided notional value when available.
	ValueUSD float64

	// Currency denotes the native currency of Value (e.g. "USDT").
	Currency string
	// Source helps identify the upstream feed when multiple channels exist.
	Source string
	// Interval describes the aggregation cadence when the upstream stream
	// exposes it (e.g. "1s", "5m"). Optional.
	Interval string

	Timestamp time.Time
	Payload   []byte
}

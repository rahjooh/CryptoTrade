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

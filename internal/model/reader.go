// Defines OrderBookSnapshotRow struct used for Parquet schema:
package model

type SpotOrderBookSnapshotPriceLevel struct {
	Price    float64
	Quantity float64
}

type SpotOrderBookSnapshot struct {
	Timestamp int64
	Bids      []SpotOrderBookSnapshotPriceLevel
	Asks      []SpotOrderBookSnapshotPriceLevel
}

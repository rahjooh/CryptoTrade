package model

type SpotOrderBookSnapshotToParquet struct {
	Timestamp int64   `parquet:"name=timestamp, type=INT64"`
	Price     float64 `parquet:"name=price, type=DOUBLE"`
	Quantity  float64 `parquet:"name=quantity, type=DOUBLE"`
}

// pipeline/flatten/flatten.go
// @tag flatten, data_transformation
package flatten

import (
	"CryptoFlow/internal/model"
)

// FlattenSpotSnapshot converts a normalized spot snapshot into a slice of flattened rows.
// @param snapshot The normalized SpotOrderBookSnapshot.
// @returns A slice of SpotSnapshotFlattenedRow pointers.
func FlattenSpotSnapshot(snapshot *model.SpotOrderBookSnapshot) []*model.SpotSnapshotFlattenedRow {
	rows := make([]*model.SpotSnapshotFlattenedRow, 0, len(snapshot.Bids)+len(snapshot.Asks))

	for i, bid := range snapshot.Bids {
		rows = append(rows, &model.SpotSnapshotFlattenedRow{
			Timestamp:  snapshot.Timestamp,
			Exchange:   snapshot.Exchange,
			Symbol:     snapshot.Symbol,
			Side:       "bid",
			Price:      bid.Price,
			Quantity:   bid.Quantity,
			Level:      i + 1,
			Source:     "snapshot",
			MarketType: "spot",
		})
	}

	for i, ask := range snapshot.Asks {
		rows = append(rows, &model.SpotSnapshotFlattenedRow{
			Timestamp:  snapshot.Timestamp,
			Exchange:   snapshot.Exchange,
			Symbol:     snapshot.Symbol,
			Side:       "ask",
			Price:      ask.Price,
			Quantity:   ask.Quantity,
			Level:      i + 1,
			Source:     "snapshot",
			MarketType: "spot",
		})
	}

	return rows
}

// FlattenFutureSnapshot converts a normalized future snapshot into a slice of flattened rows.
// @param snapshot The normalized FutureOrderBookSnapshot.
// @returns A slice of FutureSnapshotFlattenedRow pointers.
func FlattenFutureSnapshot(snapshot *model.FutureOrderBookSnapshot) []*model.FutureSnapshotFlattenedRow {
	rows := make([]*model.FutureSnapshotFlattenedRow, 0, len(snapshot.Bids)+len(snapshot.Asks))

	for i, bid := range snapshot.Bids {
		rows = append(rows, &model.FutureSnapshotFlattenedRow{
			Timestamp:  snapshot.Timestamp,
			Exchange:   snapshot.Exchange,
			Symbol:     snapshot.Symbol,
			Side:       "bid",
			Price:      bid.Price,
			Quantity:   bid.Quantity,
			Level:      i + 1,
			Source:     "snapshot",
			MarketType: "future",
		})
	}

	for i, ask := range snapshot.Asks {
		rows = append(rows, &model.FutureSnapshotFlattenedRow{
			Timestamp:  snapshot.Timestamp,
			Exchange:   snapshot.Exchange,
			Symbol:     snapshot.Symbol,
			Side:       "ask",
			Price:      ask.Price,
			Quantity:   ask.Quantity,
			Level:      i + 1,
			Source:     "snapshot",
			MarketType: "future",
		})
	}

	return rows
}

// FlattenSpotDelta converts a normalized spot delta into a slice of flattened rows.
// @param delta The normalized SpotOrderBookDelta.
// @returns A slice of SpotDeltaFlattenedRow pointers.
func FlattenSpotDelta(delta *model.SpotOrderBookDelta) []*model.SpotDeltaFlattenedRow {
	rows := make([]*model.SpotDeltaFlattenedRow, 0, len(delta.Bids)+len(delta.Asks))

	for i, bid := range delta.Bids {
		rows = append(rows, &model.SpotDeltaFlattenedRow{
			Timestamp:  delta.Timestamp,
			Exchange:   delta.Exchange,
			Symbol:     delta.Symbol,
			Side:       "bid",
			Price:      bid.Price,
			Quantity:   bid.Quantity,
			Level:      i + 1,
			Source:     "delta",
			MarketType: "spot",
		})
	}

	for i, ask := range delta.Asks {
		rows = append(rows, &model.SpotDeltaFlattenedRow{
			Timestamp:  delta.Timestamp,
			Exchange:   delta.Exchange,
			Symbol:     delta.Symbol,
			Side:       "ask",
			Price:      ask.Price,
			Quantity:   ask.Quantity,
			Level:      i + 1,
			Source:     "delta",
			MarketType: "spot",
		})
	}

	return rows
}

// FlattenFutureDelta converts a normalized future delta into a slice of flattened rows.
// @param delta The normalized FutureOrderBookDelta.
// @returns A slice of FutureDeltaFlattenedRow pointers.
func FlattenFutureDelta(delta *model.FutureOrderBookDelta) []*model.FutureDeltaFlattenedRow {
	rows := make([]*model.FutureDeltaFlattenedRow, 0, len(delta.Bids)+len(delta.Asks))

	for i, bid := range delta.Bids {
		rows = append(rows, &model.FutureDeltaFlattenedRow{
			Timestamp:  delta.Timestamp,
			Exchange:   delta.Exchange,
			Symbol:     delta.Symbol,
			Side:       "bid",
			Price:      bid.Price,
			Quantity:   bid.Quantity,
			Level:      i + 1,
			Source:     "delta",
			MarketType: "future",
		})
	}

	for i, ask := range delta.Asks {
		rows = append(rows, &model.FutureDeltaFlattenedRow{
			Timestamp:  delta.Timestamp,
			Exchange:   delta.Exchange,
			Symbol:     delta.Symbol,
			Side:       "ask",
			Price:      ask.Price,
			Quantity:   ask.Quantity,
			Level:      i + 1,
			Source:     "delta",
			MarketType: "future",
		})
	}

	return rows
}

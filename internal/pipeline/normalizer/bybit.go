package normalizer// pipeline/normalizer/bybit.go
import (
	"encoding/json"
	"strconv"
	"time"
)

// @tag normalizer, bybit
package normalizer

import (
"encoding/json"
"CryptoFlow/internal/model"
"strconv"
"time"
)

// normalizeBybitSpotSnapshot normalizes Bybit's raw spot snapshot data.
func normalizeBybitSpotSnapshot(rawData []byte, limit int) (*model.SpotOrderBookSnapshot, error) {
	var bybitSnap model.BybitSpotSnapshot
	if err := json.Unmarshal(rawData, &bybitSnap); err != nil {
		return nil, err
	}

	normalized := &model.SpotOrderBookSnapshot{
		Exchange:   "bybit",
		Symbol:     bybitSnap.Result.Symbol,
		Timestamp:  time.Unix(0, bybitSnap.Result.Ts*int64(time.Millisecond)),
		Bids:       make([]model.OrderBookEntry, 0, limit),
		Asks:       make([]model.OrderBookEntry, 0, limit),
		RawMessage: rawData,
	}

	for _, b := range bybitSnap.Result.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		quantity, _ := strconv.ParseFloat(b[1], 64)
		normalized.Bids = append(normalized.Bids, model.OrderBookEntry{Price: price, Quantity: quantity})
	}
	for _, a := range bybitSnap.Result.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		quantity, _ := strconv.ParseFloat(a[1], 64)
		normalized.Asks = append(normalized.Asks, model.OrderBookEntry{Price: price, Quantity: quantity})
	}

	return normalized, nil
}

// normalizeBybitFutureSnapshot normalizes Bybit's raw future snapshot data.
func normalizeBybitFutureSnapshot(rawData []byte, limit int) (*model.FutureOrderBookSnapshot, error) {
	var bybitSnap model.BybitFutureSnapshot
	if err := json.Unmarshal(rawData, &bybitSnap); err != nil {
		return nil, err
	}

	normalized := &model.FutureOrderBookSnapshot{
		Exchange:   "bybit",
		Symbol:     bybitSnap.Result.Symbol,
		Timestamp:  time.Unix(0, bybitSnap.Result.Ts*int64(time.Millisecond)),
		Bids:       make([]model.OrderBookEntry, 0, limit),
		Asks:       make([]model.OrderBookEntry, 0, limit),
		RawMessage: rawData,
	}

	for _, b := range bybitSnap.Result.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		quantity, _ := strconv.ParseFloat(b[1], 64)
		normalized.Bids = append(normalized.Bids, model.OrderBookEntry{Price: price, Quantity: quantity})
	}
	for _, a := range bybitSnap.Result.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		quantity, _ := strconv.ParseFloat(a[1], 64)
		normalized.Asks = append(normalized.Asks, model.OrderBookEntry{Price: price, Quantity: quantity})
	}

	return normalized, nil
}

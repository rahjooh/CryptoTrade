// pipeline/normalizer/binance.go
// @tag normalizer, binance
package normalizer

import (
	"CryptoFlow/internal/model"
	"encoding/json"
	"strconv"
	"time"
)

// normalizeBinanceSpotSnapshot normalizes Binance's raw spot snapshot data.
func normalizeBinanceSpotSnapshot(rawData []byte, limit int) (*model.SpotOrderBookSnapshot, error) {
	var binanceSnap model.BinanceSpotSnapshot
	if err := json.Unmarshal(rawData, &binanceSnap); err != nil {
		return nil, err
	}

	normalized := &model.SpotOrderBookSnapshot{
		Exchange:   "binance",
		Timestamp:  time.Now(), // Using current time as a fallback
		Bids:       make([]model.OrderBookEntry, 0, limit),
		Asks:       make([]model.OrderBookEntry, 0, limit),
		RawMessage: rawData,
	}

	for _, b := range binanceSnap.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		quantity, _ := strconv.ParseFloat(b[1], 64)
		normalized.Bids = append(normalized.Bids, model.OrderBookEntry{Price: price, Quantity: quantity})
	}
	for _, a := range binanceSnap.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		quantity, _ := strconv.ParseFloat(a[1], 64)
		normalized.Asks = append(normalized.Asks, model.OrderBookEntry{Price: price, Quantity: quantity})
	}

	return normalized, nil
}

// normalizeBinanceFutureSnapshot normalizes Binance's raw future snapshot data.
func normalizeBinanceFutureSnapshot(rawData []byte, limit int) (*model.FutureOrderBookSnapshot, error) {
	var binanceSnap model.BinanceFutureSnapshot
	if err := json.Unmarshal(rawData, &binanceSnap); err != nil {
		return nil, err
	}

	normalized := &model.FutureOrderBookSnapshot{
		Exchange:   "binance",
		Timestamp:  time.Now(),
		Bids:       make([]model.OrderBookEntry, 0, limit),
		Asks:       make([]model.OrderBookEntry, 0, limit),
		RawMessage: rawData,
	}

	for _, b := range binanceSnap.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		quantity, _ := strconv.ParseFloat(b[1], 64)
		normalized.Bids = append(normalized.Bids, model.OrderBookEntry{Price: price, Quantity: quantity})
	}
	for _, a := range binanceSnap.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		quantity, _ := strconv.ParseFloat(a[1], 64)
		normalized.Asks = append(normalized.Asks, model.OrderBookEntry{Price: price, Quantity: quantity})
	}

	return normalized, nil
}

// pipeline/normalizer/normalizer.go
// @tag normalizer, data_transformation, dispatcher
package normalizer

import (
	"CryptoFlow/internal/model"
	"fmt"
)

// NormalizeSpotSnapshot normalizes raw spot snapshot data for a given exchange.
func NormalizeSpotSnapshot(exchangeName string, rawData []byte, limit int) (*model.SpotOrderBookSnapshot, error) {
	switch exchangeName {
	case "binance":
		return normalizeBinanceSpotSnapshot(rawData, limit)
	case "bybit":
		return normalizeBybitSpotSnapshot(rawData, limit)
	// TODO: Add cases for okx, kucoin, coinbase, kraken
	default:
		return nil, fmt.Errorf("unsupported spot exchange for normalization: %s", exchangeName)
	}
}

// NormalizeFutureSnapshot normalizes raw future snapshot data for a given exchange.
func NormalizeFutureSnapshot(exchangeName string, rawData []byte, limit int) (*model.FutureOrderBookSnapshot, error) {
	switch exchangeName {
	case "binance":
		return normalizeBinanceFutureSnapshot(rawData, limit)
	case "bybit":
		return normalizeBybitFutureSnapshot(rawData, limit)
	// TODO: Add cases for okx, etc.
	default:
		return nil, fmt.Errorf("unsupported future exchange for normalization: %s", exchangeName)
	}
}

// NormalizeSpotDelta normalizes raw spot delta data.
func NormalizeSpotDelta(exchangeName string, rawData []byte) (*model.SpotOrderBookDelta, error) {
	// TODO: Implement delta normalization logic here.
	return nil, nil
}

// NormalizeFutureDelta normalizes raw future delta data.
func NormalizeFutureDelta(exchangeName string, rawData []byte) (*model.FutureOrderBookDelta, error) {
	// TODO: Implement delta normalization logic here.
	return nil, nil
}

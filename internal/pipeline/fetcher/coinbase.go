// internal/pipeline/fetcher/coinbase.go
// @tag fetcher, coinbase
package fetcher

import (
	"CryptoFlow/internal/model"
	"fmt"
)

// fetchCoinbase fetches data from the Coinbase API.
func fetchCoinbase(url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) ([]byte, error) {
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookSnapshot {
			return fetchCoinbaseSpotSnapshot(url, symbol, limit)
		}
	}
	return nil, fmt.Errorf("unsupported data type or market for Coinbase: %s - %s", marketType, dataType)
}

// fetchCoinbaseSpotSnapshot fetches a spot order book snapshot from Coinbase.
func fetchCoinbaseSpotSnapshot(url, symbol string, limit int) ([]byte, error) {
	// Coinbase's API might have a different parameter name for symbol, adjust as needed.
	fullURL := fmt.Sprintf("%s?level=2", url)
	return httpGet(fullURL)
}

// TODO: Implement Coinbase WebSocket logic for deltas.
// func fetchCoinbaseWebSocket(url, symbol string, marketType model.MarketType, dataType model.DataSourceType) error {
// 	// Implementation for Coinbase WebSocket client.
// 	return nil
// }

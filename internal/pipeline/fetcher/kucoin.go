// internal/pipeline/fetcher/kucoin.go
// @tag fetcher, kucoin
package fetcher

import (
	"CryptoFlow/internal/model"
	"fmt"
)

// fetchKucoin fetches data from the Kucoin API.
func fetchKucoin(url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) ([]byte, error) {
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookSnapshot {
			return fetchKucoinSpotSnapshot(url, symbol, limit)
		}
	}
	return nil, fmt.Errorf("unsupported data type or market for Kucoin: %s - %s", marketType, dataType)
}

// fetchKucoinSpotSnapshot fetches a spot order book snapshot from Kucoin.
func fetchKucoinSpotSnapshot(url, symbol string, limit int) ([]byte, error) {
	// Kucoin's API might have a different parameter name for symbol, adjust as needed.
	fullURL := fmt.Sprintf("%s?symbol=%s", url, symbol)
	return httpGet(fullURL)
}

// TODO: Implement Kucoin WebSocket logic for deltas.
// func fetchKucoinWebSocket(url, symbol string, marketType model.MarketType, dataType model.DataSourceType) error {
// 	// Implementation for Kucoin WebSocket client.
// 	return nil
// }

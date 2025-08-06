// internal/pipeline/fetcher/kraken.go
// @tag fetcher, kraken
package fetcher

import (
	"CryptoFlow/internal/model"
	"fmt"
)

// fetchKraken fetches data from the Kraken API.
func fetchKraken(url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) ([]byte, error) {
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookSnapshot {
			return fetchKrakenSpotSnapshot(url, symbol, limit)
		}
	}
	return nil, fmt.Errorf("unsupported data type or market for Kraken: %s - %s", marketType, dataType)
}

// fetchKrakenSpotSnapshot fetches a spot order book snapshot from Kraken.
func fetchKrakenSpotSnapshot(url, symbol string, limit int) ([]byte, error) {
	// Kraken's API might have a different parameter name for symbol, adjust as needed.
	fullURL := fmt.Sprintf("%s?pair=%s&count=%d", url, symbol, limit)
	return httpGet(fullURL)
}

// TODO: Implement Kraken WebSocket logic for deltas.
// func fetchKrakenWebSocket(url, symbol string, marketType model.MarketType, dataType model.DataSourceType) error {
// 	// Implementation for Kraken WebSocket client.
// 	return nil
// }

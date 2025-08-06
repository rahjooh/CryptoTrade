// internal/reader/fetcher/bybit.go
// @tag fetcher, bybit, websocket
package fetcher

import (
	"CryptoFlow/internal/model"
	"fmt"

	"github.com/gorilla/websocket"
)

// bybitRest fetches data from the Bybit API.
func bybitRest(url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) ([]byte, error) {
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookSnapshot {
			fullURL := fmt.Sprintf("%s?symbol=%s&limit=%d", url, symbol, limit)
			return httpGet(fullURL)
		}
	case model.MarketTypeFuture:
		if dataType == model.DataSourceOrderbookSnapshot {
			fullURL := fmt.Sprintf("%s?category=linear&symbol=%s&limit=%d", url, symbol, limit)
			return httpGet(fullURL)
		}
	}
	return nil, fmt.Errorf("unsupported data type or market for Bybit: %s - %s", marketType, dataType)
}

// bybitWebsocket sends a subscription message to Bybit.
func bybitWebsocket(conn *websocket.Conn, exchangeName, symbol string, marketType model.MarketType, dataType model.DataSourceType) error {
	var message map[string]interface{}
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookDelta {
			message = map[string]interface{}{
				"op":   "subscribe",
				"args": []string{fmt.Sprintf("orderbook.1.%s", symbol)},
			}
		}
	case model.MarketTypeFuture:
		if dataType == model.DataSourceOrderbookDelta {
			message = map[string]interface{}{
				"op":   "subscribe",
				"args": []string{fmt.Sprintf("orderbook.1.%s", symbol)},
			}
		}
	default:
		return fmt.Errorf("unsupported subscription for Bybit")
	}

	return conn.WriteJSON(message)
}

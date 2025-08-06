// internal/reader/fetcher/binance.go
// @tag fetcher, binance, websocket
package fetcher

import (
	"CryptoFlow/internal/model"
	"fmt"
	"github.com/gorilla/websocket"
)

// binanceRest fetches data from the Binance API.
func binanceRest(url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) ([]byte, error) {
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookSnapshot {
			fullURL := fmt.Sprintf("%s?symbol=%s&limit=%d", url, symbol, limit)
			return httpGet(fullURL)
		}
	case model.MarketTypeFuture:
		if dataType == model.DataSourceOrderbookSnapshot {
			fullURL := fmt.Sprintf("%s?symbol=%s&limit=%d", url, symbol, limit)
			return httpGet(fullURL)
		}
	}
	return nil, fmt.Errorf("unsupported data type or market for Binance: %s - %s", marketType, dataType)
}

// binanceWebsocket sends a subscription message to Binance.
func binanceWebsocket(conn *websocket.Conn, exchangeName, symbol string, marketType model.MarketType, dataType model.DataSourceType) error {
	var message map[string]interface{}
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookDelta {
			message = map[string]interface{}{
				"method": "SUBSCRIBE",
				"params": []string{
					fmt.Sprintf("%s@depth", symbol),
				},
				"id": 1,
			}
		}
	case model.MarketTypeFuture:
		if dataType == model.DataSourceOrderbookDelta {
			message = map[string]interface{}{
				"method": "SUBSCRIBE",
				"params": []string{
					fmt.Sprintf("%s@depth", symbol),
				},
				"id": 1,
			}
		}
	default:
		return fmt.Errorf("unsupported subscription for Binance")
	}

	return conn.WriteJSON(message)
}

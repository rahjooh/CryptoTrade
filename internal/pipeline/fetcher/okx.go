// internal/reader/fetcher/okx.go
// @tag fetcher, okx, websocket
package fetcher

import (
	"CryptoFlow/internal/model"
	"fmt"

	"github.com/gorilla/websocket"
)

// okxRest fetches data from the OKX API.
func okxRest(url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) ([]byte, error) {
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookSnapshot {
			fullURL := fmt.Sprintf("%s?instId=%s&sz=%d", url, symbol, limit)
			return httpGet(fullURL)
		}
	case model.MarketTypeFuture:
		if dataType == model.DataSourceOrderbookSnapshot {
			fullURL := fmt.Sprintf("%s?instId=%s&sz=%d", url, symbol, limit)
			return httpGet(fullURL)
		}
	}
	return nil, fmt.Errorf("unsupported data type or market for OKX: %s - %s", marketType, dataType)
}

// sendSubscriptionMessage sends a subscription message to OKX.
func okxWebsocket(conn *websocket.Conn, exchangeName, symbol string, marketType model.MarketType, dataType model.DataSourceType) error {
	var message map[string]interface{}
	switch marketType {
	case model.MarketTypeSpot:
		if dataType == model.DataSourceOrderbookDelta {
			message = map[string]interface{}{
				"op": "subscribe",
				"args": []map[string]string{
					{
						"channel": "books",
						"instId":  symbol,
					},
				},
			}
		}
	case model.MarketTypeFuture:
		if dataType == model.DataSourceOrderbookDelta {
			message = map[string]interface{}{
				"op": "subscribe",
				"args": []map[string]string{
					{
						"channel": "books",
						"instId":  symbol,
					},
				},
			}
		}
	default:
		return fmt.Errorf("unsupported subscription for OKX")
	}

	return conn.WriteJSON(message)
}

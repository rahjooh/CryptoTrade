// internal/reader/fetcher/fetcher.go
// @tag fetcher, api, rest, websocket
package fetcher

import (
	"CryptoFlow/internal/config" // Make sure this import is here
	"CryptoFlow/internal/logger"
	"CryptoFlow/internal/model"
	"CryptoFlow/internal/reader/flatten"
	"CryptoFlow/internal/reader/normalizer"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RunRestFetcher fetches data from a REST API at regular intervals.
// It now handles the full data processing pipeline (normalize, sort, flatten).
func RunRestFetcher(cfg *config.SourceConf, exchangeName string, source *config.Configs, symbol string, flattenedOutput chan<- interface{}, wg *sync.WaitGroup, marketType model.MarketType, dataType model.DataSourceType) { // <--- Changed cfg type
	defer wg.Done()
	logger.Infof("[%s][%s] Starting REST fetcher for %s on %s every %dms", marketType, dataType, symbol, exchangeName, source.IntervalMS) // Use IntervalMS

	ticker := time.NewTicker(time.Duration(source.IntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if rawData, err := fetchREST(exchangeName, source.URL, symbol, source.Limit, marketType, dataType); err != nil {
			logger.Errorf("[%s][%s] Error fetching data for %s on %s: %v", marketType, dataType, symbol, exchangeName, err)
		} else {
			processAndFlatten(cfg, rawData, flattenedOutput, exchangeName, marketType, dataType, symbol)
		}
	}
}

// RunWebSocketClient handles real-time data via WebSocket.
// The `dialer` parameter is new to enable testability.
func RunWebSocketClient(cfg *config.SourceConf, exchangeName string, source *config.Configs, symbol string, flattenedOutput chan<- interface{}, wg *sync.WaitGroup, marketType model.MarketType, dataType model.DataSourceType, dialer *websocket.Dialer) { // <--- Changed cfg type
	defer wg.Done()
	logger.Infof("[%s][%s] Starting WebSocket listener for %s on %s", marketType, dataType, symbol, exchangeName)

	var conn *websocket.Conn
	var err error

	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	for {
		conn, _, err = dialer.Dial(source.URL, nil)
		if err != nil {
			logger.Errorf("[%s][%s] Failed to connect to WebSocket for %s on %s: %v", marketType, dataType, symbol, exchangeName, err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()

		if err := sendSubscriptionMessage(conn, exchangeName, symbol, marketType, dataType); err != nil {
			logger.Errorf("[%s][%s] Failed to subscribe to WebSocket for %s on %s: %v", marketType, dataType, symbol, exchangeName, err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		logger.Infof("[%s][%s] Subscribed to %s on %s", marketType, dataType, symbol, exchangeName)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				logger.Errorf("[%s][%s] Read error for %s on %s: %v", marketType, dataType, symbol, exchangeName, err)
				break
			}

			rawData := &model.RawData{
				Exchange:   exchangeName,
				MarketType: marketType,
				DataType:   dataType,
				Symbol:     symbol,
				Data:       message,
			}
			processAndFlatten(cfg, rawData, flattenedOutput, exchangeName, marketType, dataType, symbol)
		}

		time.Sleep(5 * time.Second)
	}
}

// processAndFlatten handles the full processing pipeline for a raw data message.
func processAndFlatten(cfg *config.SourceConf, rawData *model.RawData, flattenedOutput chan<- interface{}, exchangeName string, marketType model.MarketType, dataType model.DataSourceType, symbol string) { // <--- Changed cfg type
	// Filter unneeded symbols for deltas
	if dataType == model.DataSourceOrderbookDelta && !isSymbolNeeded(cfg, exchangeName, marketType, symbol) {
		return
	}

	normalizedData, err := normalizer.Normalize(rawData)
	if err != nil {
		logger.Errorf("[%s][%s] Error normalizing raw data for %s on %s: %v", marketType, dataType, symbol, exchangeName, err)
		return
	}

	flattenedRows, err := flatten.Flatten(normalizedData)
	if err != nil {
		logger.Errorf("[%s][%s] Error flattening normalized data for %s on %s: %v", marketType, dataType, symbol, exchangeName, err)
		return
	}

	for _, row := range flattenedRows {
		flattenedOutput <- row
	}
}

// fetchREST fetches data from a REST API for the specified exchange.
func fetchREST(exchangeName, url, symbol string, limit int, marketType model.MarketType, dataType model.DataSourceType) (*model.RawData, error) {
	var rawData []byte
	var err error

	switch exchangeName {
	case "binance":
		rawData, err = fetchBinance(url, symbol, limit, marketType, dataType)
	case "bybit":
		rawData, err = fetchBybit(url, symbol, limit, marketType, dataType)
	case "okx":
		rawData, err = fetchOKX(url, symbol, limit, marketType, dataType)
	case "kucoin":
		rawData, err = fetchKucoin(url, symbol, limit, marketType, dataType)
	case "coinbase":
		rawData, err = fetchCoinbase(url, symbol, limit, marketType, dataType)
	case "kraken":
		rawData, err = fetchKraken(url, symbol, limit, marketType, dataType)
	default:
		return nil, fmt.Errorf("unsupported exchange: %s", exchangeName)
	}

	if err != nil {
		return nil, err
	}

	return &model.RawData{
		Exchange:   exchangeName,
		MarketType: marketType,
		DataType:   dataType,
		Symbol:     symbol,
		Data:       rawData,
	}, nil
}

// httpGet is a robust helper function for making HTTP GET requests.
func httpGet(url string) ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("User-Agent", "CryptoFlow/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}

// isSymbolNeeded checks the config to see if a symbol is configured to be processed.
func isSymbolNeeded(cfg *config.SourceConf, exchange string, marketType model.MarketType, symbol string) bool { // <--- Changed cfg type
	if exchangeConfig, ok := cfg.Source[exchange]; ok {
		if marketType == model.MarketTypeSpot && exchangeConfig.Spot != nil && exchangeConfig.Spot.Orderbook != nil && exchangeConfig.Spot.Orderbook.Delta != nil {
			for _, s := range exchangeConfig.Spot.Orderbook.Delta.Symbols {
				if s == symbol {
					return true
				}
			}
		}
		if marketType == model.MarketTypeFuture && exchangeConfig.Future != nil && exchangeConfig.Future.Orderbook != nil && exchangeConfig.Future.Orderbook.Delta != nil {
			for _, s := range exchangeConfig.Future.Orderbook.Delta.Symbols {
				if s == symbol {
					return true
				}
			}
		}
	}
	return false
}

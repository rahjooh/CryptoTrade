// internal/pipeline/pipeline.go
// @tag reader, concurrency, pipeline
package pipeline // Ensure this is 'pipeline'

import (
	"CryptoFlow/internal/config"
	"CryptoFlow/internal/logger"
	"CryptoFlow/internal/model"
	"sync"
)

// StartReaders initializes and runs all data fetching tasks based on the config.
// It fetches raw data, processes it, and pushes flattened data into the output channel.
// @param cfg The application configuration.
// @param flattenedOutput The channel to push flattened data into.
// @param wg The WaitGroup to track all running goroutines.
func StartReaders(cfg *config.Configs, flattenedOutput chan<- interface{}, wg *sync.WaitGroup) {
	logger.Info("Initializing data readers...")
	for _, exchangeConfig := range cfg.Exchanges {
		logger.Infof("Starting readers for exchange: %s", exchangeConfig.Name)
		if exchangeConfig.Spot != nil {
			handleSpotConfig(cfg, exchangeConfig.Name, exchangeConfig.Spot, flattenedOutput, wg)
		}
		if exchangeConfig.Future != nil {
			handleFutureConfig(cfg, exchangeConfig.Name, exchangeConfig.Future, flattenedOutput, wg)
		}
	}
}

// handleSpotConfig dispatches goroutines for spot market data.
func handleSpotConfig(cfg *config.Configs, exchangeName string, spotConfig *config.SpotConfig, flattenedOutput chan<- interface{}, wg *sync.WaitGroup) {
	if spotConfig.OrderbookSnapshots.Enabled {
		for _, symbol := range spotConfig.OrderbookSnapshots.Symbols {
			wg.Add(1)
			go fetcher.RunRestFetcher(cfg, exchangeName, spotConfig.OrderbookSnapshots, symbol, flattenedOutput, wg, model.MarketTypeSpot, model.DataSourceOrderbookSnapshot)
		}
	}
	if spotConfig.OrderbookDelta.Enabled {
		for _, symbol := range spotConfig.OrderbookDelta.Symbols {
			wg.Add(1)
			go fetcher.RunWebSocketClient(cfg, exchangeName, spotConfig.OrderbookDelta, symbol, flattenedOutput, wg, model.MarketTypeSpot, model.DataSourceOrderbookDelta, nil)
		}
	}
}

// handleFutureConfig dispatches goroutines for future market data.
func handleFutureConfig(cfg *config.Config, exchangeName string, futureConfig *config.FutureConfig, flattenedOutput chan<- interface{}, wg *sync.WaitGroup) {
	if futureConfig.OrderbookSnapshots.Enabled {
		for _, symbol := range futureConfig.OrderbookSnapshots.Symbols {
			wg.Add(1)
			go fetcher.RunRestFetcher(cfg, exchangeName, futureConfig.OrderbookSnapshots, symbol, flattenedOutput, wg, model.MarketTypeFuture, model.DataSourceOrderbookSnapshot)
		}
	}
	if futureConfig.OrderbookDelta.Enabled {
		for _, symbol := range futureConfig.OrderbookDelta.Symbols {
			wg.Add(1)
			go fetcher.RunWebSocketClient(cfg, exchangeName, futureConfig.OrderbookDelta, symbol, flattenedOutput, wg, model.MarketTypeFuture, model.DataSourceOrderbookDelta, nil)
		}
	}
}

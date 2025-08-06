package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"
)

type BinanceReader struct {
	config     *config.Config
	client     *http.Client
	rawChannel chan<- models.RawOrderbookMessage
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Logger

	// Metrics
	requestCount     int64
	errorCount       int64
	lastRequestTime  time.Time
	lastResponseTime time.Time
}

func NewBinanceReader(cfg *config.Config, rawChannel chan<- models.RawOrderbookMessage) *BinanceReader {
	log := logger.GetLogger()

	transport := &http.Transport{
		MaxIdleConns:       cfg.Exchanges.Binance.ConnectionPool.MaxIdleConns,
		MaxConnsPerHost:    cfg.Exchanges.Binance.ConnectionPool.MaxConnsPerHost,
		IdleConnTimeout:    cfg.Exchanges.Binance.ConnectionPool.IdleConnTimeout,
		DisableCompression: false,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.Reader.Timeout,
	}

	reader := &BinanceReader{
		config:     cfg,
		client:     client,
		rawChannel: rawChannel,
		wg:         &sync.WaitGroup{},
		log:        log,
	}

	log.WithComponent("binance_reader").WithFields(logger.Fields{
		"max_idle_conns":     cfg.Exchanges.Binance.ConnectionPool.MaxIdleConns,
		"max_conns_per_host": cfg.Exchanges.Binance.ConnectionPool.MaxConnsPerHost,
		"timeout":            cfg.Reader.Timeout,
	}).Info("binance reader initialized")

	return reader
}

func (br *BinanceReader) Start(ctx context.Context) error {
	br.mu.Lock()
	if br.running {
		br.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	br.running = true
	br.ctx = ctx
	br.mu.Unlock()

	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{"operation": "start"})

	if !br.config.Exchanges.Binance.Enabled {
		log.Warn("binance exchange is disabled")
		return fmt.Errorf("binance exchange is disabled")
	}

	log.Info("starting binance reader")

	// Start workers for each symbol
	for _, endpoint := range br.config.Exchanges.Binance.Endpoints {
		if endpoint.Name == "futures_depth" {
			log.WithFields(logger.Fields{
				"endpoint": endpoint.Name,
				"symbols":  endpoint.Symbols,
				"interval": endpoint.IntervalMs,
			}).Info("starting endpoint workers")

			for _, symbol := range endpoint.Symbols {
				br.wg.Add(1)
				go br.fetchOrderbookWorker(symbol, endpoint)
			}
		}
	}

	// Start metrics reporter
	go br.metricsReporter(ctx)

	log.Info("binance reader started successfully")
	return nil
}

func (br *BinanceReader) Stop() {
	br.mu.Lock()
	br.running = false
	br.mu.Unlock()

	br.log.WithComponent("binance_reader").Info("stopping binance reader")
	br.wg.Wait()
	br.log.WithComponent("binance_reader").Info("binance reader stopped")
}

func (br *BinanceReader) fetchOrderbookWorker(symbol string, endpoint config.EndpointConfig) {
	defer br.wg.Done()

	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{
		"symbol":   symbol,
		"endpoint": endpoint.Name,
		"worker":   "orderbook_fetcher",
	})

	log.Info("starting orderbook worker")

	ticker := time.NewTicker(time.Duration(endpoint.IntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-br.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case <-ticker.C:
			start := time.Now()
			br.fetchOrderbook(symbol, endpoint)
			duration := time.Since(start)

			if duration > time.Duration(endpoint.IntervalMs)*time.Millisecond {
				log.WithFields(logger.Fields{
					"duration": duration.Milliseconds(),
					"interval": endpoint.IntervalMs,
				}).Warn("fetch took longer than interval")
			}
		}
	}
}

func (br *BinanceReader) fetchOrderbook(symbol string, endpoint config.EndpointConfig) {
	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"endpoint":  endpoint.Name,
		"operation": "fetch_orderbook",
	})

	br.lastRequestTime = time.Now()
	br.requestCount++

	url := fmt.Sprintf("%s%s?symbol=%s&limit=%d",
		br.config.Exchanges.Binance.BaseURL,
		endpoint.Path,
		symbol,
		endpoint.Limit)

	log.WithFields(logger.Fields{"url": url}).Debug("making API request")

	req, err := http.NewRequestWithContext(br.ctx, "GET", url, nil)
	if err != nil {
		br.sendError(symbol, fmt.Errorf("failed to create request: %w", err))
		return
	}

	resp, err := br.client.Do(req)
	if err != nil {
		br.sendError(symbol, fmt.Errorf("failed to fetch orderbook: %w", err))
		return
	}
	defer resp.Body.Close()

	br.lastResponseTime = time.Now()
	requestDuration := br.lastResponseTime.Sub(br.lastRequestTime)

	logger.LogPerformanceEntry(log, "binance_reader", "api_request", requestDuration, logger.Fields{
		"symbol":      symbol,
		"status_code": resp.StatusCode,
	})

	if resp.StatusCode != http.StatusOK {
		br.sendError(symbol, fmt.Errorf("unexpected status code: %d", resp.StatusCode))
		return
	}

	var binanceResp models.BinanceOrderbookResponse
	if err := json.NewDecoder(resp.Body).Decode(&binanceResp); err != nil {
		br.sendError(symbol, fmt.Errorf("failed to decode response: %w", err))
		return
	}

	log.WithFields(logger.Fields{
		"bids_count":     len(binanceResp.Bids),
		"asks_count":     len(binanceResp.Asks),
		"last_update_id": binanceResp.LastUpdateID,
	}).Debug("received orderbook data")

	// Validate data
	if br.config.Reader.Validation.EnablePriceValidation {
		if !br.validatePrices(binanceResp, symbol) {
			br.sendError(symbol, fmt.Errorf("price validation failed"))
			return
		}
	}

	payload, err := json.Marshal(map[string]interface{}{
		"last_update_id": binanceResp.LastUpdateID,
		"bids":           binanceResp.Bids,
		"asks":           binanceResp.Asks,
	})
	if err != nil {
		br.sendError(symbol, fmt.Errorf("failed to marshal orderbook: %w", err))
		return
	}

	rawData := models.RawOrderbookMessage{
		Exchange:    "binance",
		Symbol:      symbol,
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}

	select {
	case br.rawChannel <- rawData:
		log.Debug("orderbook data sent to raw channel")
		logger.LogDataFlowEntry(log, "binance_api", "raw_channel", len(binanceResp.Bids)+len(binanceResp.Asks), "orderbook_entries")
	case <-br.ctx.Done():
		return
	default:
		br.sendError(symbol, fmt.Errorf("raw channel is full, dropping data"))
	}
}

func (br *BinanceReader) validatePrices(resp models.BinanceOrderbookResponse, symbol string) bool {
	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{"operation": "price_validation"})

	if len(resp.Bids) == 0 || len(resp.Asks) == 0 {
		log.Warn("empty bids or asks in response")
		return false
	}

	// Check if best bid is less than best ask
	if len(resp.Bids[0]) >= 2 && len(resp.Asks[0]) >= 2 {
		bestBid, err1 := strconv.ParseFloat(resp.Bids[0][0], 64)
		bestAsk, err2 := strconv.ParseFloat(resp.Asks[0][0], 64)

		if err1 != nil || err2 != nil {
			log.WithFields(logger.Fields{
				"best_bid_str": resp.Bids[0][0],
				"best_ask_str": resp.Asks[0][0],
				"parse_error":  fmt.Sprintf("bid_err: %v, ask_err: %v", err1, err2),
			}).Error("failed to parse best bid/ask prices")
			return false
		}

		if bestBid >= bestAsk {
			log.WithFields(logger.Fields{
				"best_bid": bestBid,
				"best_ask": bestAsk,
			}).Error("best bid is greater than or equal to best ask")
			return false
		}

		// Check spread percentage
		spread := (bestAsk - bestBid) / bestBid * 100
		if spread > br.config.Reader.Validation.MaxSpreadPercentage {
			log.WithFields(logger.Fields{
				"spread":     spread,
				"max_spread": br.config.Reader.Validation.MaxSpreadPercentage,
				"best_bid":   bestBid,
				"best_ask":   bestAsk,
			}).Warn("spread exceeds maximum allowed percentage")
			return false
		}

		log.WithFields(logger.Fields{
			"best_bid": bestBid,
			"best_ask": bestAsk,
			"spread":   spread,
		}).Debug("price validation passed")
	}

	return true
}

func (br *BinanceReader) sendError(symbol string, err error) {
	br.errorCount++

	log := br.log.WithComponent("binance_reader").WithError(err)
	log.Error("error occurred during orderbook fetch")

	rawData := models.RawOrderbookMessage{
		Exchange:    "binance",
		Symbol:      symbol,
		Timestamp:   time.Now().UTC(),
		MessageType: "error",
		Data:        []byte(err.Error()),
	}

	select {
	case br.rawChannel <- rawData:
		log.Debug("error sent to raw channel")
	default:
		log.Error("raw channel full, dropping error message")
	}
}

func (br *BinanceReader) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			br.reportMetrics()
		}
	}
}

func (br *BinanceReader) reportMetrics() {
	br.mu.RLock()
	requestCount := br.requestCount
	errorCount := br.errorCount
	br.mu.RUnlock()

	errorRate := float64(0)
	if requestCount > 0 {
		errorRate = float64(errorCount) / float64(requestCount)
	}

	log := br.log.WithComponent("binance_reader")
	log.LogMetric("binance_reader", "request_count", requestCount, logger.Fields{})
	log.LogMetric("binance_reader", "error_count", errorCount, logger.Fields{})
	log.LogMetric("binance_reader", "error_rate", errorRate, logger.Fields{})

	log.WithFields(logger.Fields{
		"request_count": requestCount,
		"error_count":   errorCount,
		"error_rate":    errorRate,
	}).Info("binance reader metrics")
}

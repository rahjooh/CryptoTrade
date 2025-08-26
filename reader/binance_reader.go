package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"

	futures "github.com/adshao/go-binance/v2/futures"
)

// BinanceReader fetches futures order book snapshots from Binance.
type BinanceReader struct {
	config     *config.Config
	client     *futures.Client
	rawChannel chan<- models.RawOrderbookMessage
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log
}

// NewBinanceReader creates a new BinanceReader using the binance-go client.
func NewBinanceReader(cfg *config.Config, rawChannel chan<- models.RawOrderbookMessage) *BinanceReader {
	log := logger.GetLogger()

	transport := &http.Transport{
		MaxIdleConns:       cfg.Source.Binance.ConnectionPool.MaxIdleConns,
		MaxConnsPerHost:    cfg.Source.Binance.ConnectionPool.MaxConnsPerHost,
		IdleConnTimeout:    cfg.Source.Binance.ConnectionPool.IdleConnTimeout,
		DisableCompression: false,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.Reader.Timeout,
	}

	client := futures.NewClient("", "")
	client.HTTPClient = httpClient

	snapshotCfg := cfg.Source.Binance.Future.Orderbook.Snapshots
	if parsed, err := url.Parse(snapshotCfg.URL); err == nil {
		base := fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
		client.SetApiEndpoint(base)
	}

	reader := &BinanceReader{
		config:     cfg,
		client:     client,
		rawChannel: rawChannel,
		wg:         &sync.WaitGroup{},
		log:        log,
	}

	log.WithComponent("binance_reader").WithFields(logger.Fields{
		"max_idle_conns":     cfg.Source.Binance.ConnectionPool.MaxIdleConns,
		"max_conns_per_host": cfg.Source.Binance.ConnectionPool.MaxConnsPerHost,
		"timeout":            cfg.Reader.Timeout,
	}).Info("binance reader initialized")

	return reader
}

// Start begins fetching order book snapshots for configured symbols.
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

	snapshotCfg := br.config.Source.Binance.Future.Orderbook.Snapshots
	if !snapshotCfg.Enabled {
		log.Warn("binance futures orderbook snapshots are disabled")
		return fmt.Errorf("binance futures orderbook snapshots are disabled")
	}

	log.WithFields(logger.Fields{
		"symbols":  snapshotCfg.Symbols,
		"interval": snapshotCfg.IntervalMs,
	}).Info("starting binance reader")

	for _, symbol := range snapshotCfg.Symbols {
		br.wg.Add(1)
		go br.fetchOrderbookWorker(symbol, snapshotCfg)
	}

	log.Info("binance reader started successfully")
	return nil
}

// Stop signals all workers to stop and waits for completion.
func (br *BinanceReader) Stop() {
	br.mu.Lock()
	br.running = false
	br.mu.Unlock()

	br.log.WithComponent("binance_reader").Info("stopping binance reader")
	br.wg.Wait()
	br.log.WithComponent("binance_reader").Info("binance reader stopped")
}

func (br *BinanceReader) fetchOrderbookWorker(symbol string, snapshotCfg config.BinanceSnapshotConfig) {
	defer br.wg.Done()

	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "orderbook_fetcher",
	})

	log.Info("starting orderbook worker")

	interval := time.Duration(snapshotCfg.IntervalMs) * time.Millisecond

	now := time.Now()
	nextTick := now.Truncate(interval).Add(interval)
	timer := time.NewTimer(nextTick.Sub(now))
	defer timer.Stop()

	for {
		select {
		case <-br.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case <-timer.C:
			start := time.Now()
			br.fetchOrderbook(symbol, snapshotCfg)
			duration := time.Since(start)

			if duration > interval {
				log.WithFields(logger.Fields{
					"duration": duration.Milliseconds(),
					"interval": snapshotCfg.IntervalMs,
				}).Warn("fetch took longer than interval")
			}

			nextTick = start.Truncate(interval).Add(interval)
			timer.Reset(time.Until(nextTick))
		}
	}
}

func (br *BinanceReader) fetchOrderbook(symbol string, snapshotCfg config.BinanceSnapshotConfig) {
	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_orderbook",
	})

	market := "future-orderbook-snapshot"

	start := time.Now()
	res, err := br.client.NewDepthService().
		Symbol(symbol).
		Limit(snapshotCfg.Limit).
		Do(br.ctx)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook")
		return
	}
	duration := time.Since(start)
	logger.LogPerformanceEntry(log, "binance_reader", "api_request", duration, logger.Fields{
		"symbol": symbol,
	})

	bids := make([][]string, len(res.Bids))
	for i, b := range res.Bids {
		bids[i] = []string{b.Price, b.Quantity}
	}
	asks := make([][]string, len(res.Asks))
	for i, a := range res.Asks {
		asks[i] = []string{a.Price, a.Quantity}
	}

	binanceResp := models.BinanceOrderbookResponse{
		LastUpdateID: res.LastUpdateID,
		Bids:         bids,
		Asks:         asks,
	}

	payload, err := json.Marshal(binanceResp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}

	rawData := models.RawOrderbookMessage{
		Exchange:    "binance",
		Symbol:      symbol,
		Market:      market,
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}

	select {
	case br.rawChannel <- rawData:
		log.Info("orderbook data sent to raw channel")
		logger.LogDataFlowEntry(log, "binance_api", "raw_channel", len(binanceResp.Bids)+len(binanceResp.Asks), "orderbook_entries")
	case <-br.ctx.Done():
		return
	default:
		log.Warn("raw channel is full, dropping data")
	}
}

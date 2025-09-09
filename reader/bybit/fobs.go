package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobs "cryptoflow/internal/channel/fobs"
	"cryptoflow/logger"
	"cryptoflow/models"

	bybit "github.com/bybit-exchange/bybit.go.api"
)

// Bybit_FOBS_Reader fetches futures order book snapshots from Bybit.
type Bybit_FOBS_Reader struct {
	config   *appconfig.Config
	client   *bybit.Client
	channels *fobs.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
}

// Bybit_FOBS_NewReader creates a new snapshot reader for Bybit futures.
func Bybit_FOBS_NewReader(cfg *appconfig.Config, ch *fobs.Channels, symbols []string, localIP string) *Bybit_FOBS_Reader {
	log := logger.GetLogger()

	transport := &http.Transport{
		MaxIdleConns:        cfg.Source.Bybit.ConnectionPool.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.Source.Bybit.ConnectionPool.MaxIdleConns,
		MaxConnsPerHost:     cfg.Source.Bybit.ConnectionPool.MaxConnsPerHost,
		IdleConnTimeout:     cfg.Source.Bybit.ConnectionPool.IdleConnTimeout,
		DisableCompression:  false,
	}
	if localIP != "" {
		if ip := net.ParseIP(localIP); ip != nil {
			dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}
			transport.DialContext = dialer.DialContext
		}
	}

	httpClient := &http.Client{Transport: transport, Timeout: cfg.Reader.Timeout}

	snapshotCfg := cfg.Source.Bybit.Future.Orderbook.Snapshots
	base := snapshotCfg.URL
	if parsed, err := url.Parse(snapshotCfg.URL); err == nil {
		base = fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
	}

	client := bybit.NewBybitHttpClient("", "", bybit.WithBaseURL(base))
	client.HTTPClient = httpClient

	r := &Bybit_FOBS_Reader{
		config:   cfg,
		client:   client,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      log,
		symbols:  symbols,
	}

	log.WithComponent("bybit_reader").WithFields(logger.Fields{
		"timeout": cfg.Reader.Timeout,
	}).Info("bybit snapshot reader initialized")

	return r
}

// Bybit_FOBS_Start begins fetching order book snapshots for configured symbols.
func (r *Bybit_FOBS_Reader) Bybit_FOBS_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	log := r.log.WithComponent("bybit_reader").WithFields(logger.Fields{"operation": "Bybit_FOBS_Start"})

	cfg := r.config.Source.Bybit.Future.Orderbook.Snapshots
	if !cfg.Enabled {
		log.Warn("bybit futures orderbook snapshots are disabled")
		return fmt.Errorf("bybit futures orderbook snapshots are disabled")
	}

	log.WithFields(logger.Fields{
		"symbols":  r.symbols,
		"interval": cfg.IntervalMs,
	}).Info("starting bybit snapshot reader")

	for _, sym := range r.symbols {
		r.wg.Add(1)
		go r.fetchOrderbookWorker(sym, cfg)
	}

	log.Info("bybit snapshot reader started successfully")
	return nil
}

// Bybit_FOBS_Stop signals workers to stop.
func (r *Bybit_FOBS_Reader) Bybit_FOBS_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("bybit_reader").Info("stopping bybit snapshot reader")
	r.wg.Wait()
	r.log.WithComponent("bybit_reader").Info("bybit snapshot reader stopped")
}

func (r *Bybit_FOBS_Reader) fetchOrderbookWorker(symbol string, snapshotCfg appconfig.BybitSnapshotConfig) {
	defer r.wg.Done()

	log := r.log.WithComponent("bybit_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "orderbook_fetcher",
	})
	log.Info("starting orderbook worker")

	interval := time.Duration(snapshotCfg.IntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case <-ticker.C:
			r.fetchOrderbook(symbol, snapshotCfg)
		}
	}
}

func (r *Bybit_FOBS_Reader) fetchOrderbook(symbol string, snapshotCfg appconfig.BybitSnapshotConfig) {
	log := r.log.WithComponent("bybit_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_orderbook",
	})

	params := map[string]interface{}{
		"category": "linear",
		"symbol":   symbol,
		"limit":    snapshotCfg.Limit,
	}

	start := time.Now()
	resp, err := r.client.NewUtaBybitServiceWithParams(params).GetOrderBookInfo(r.ctx)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook")
		return
	}
	duration := time.Since(start)
	logger.LogPerformanceEntry(log, "bybit_reader", "api_request", duration, logger.Fields{"symbol": symbol})

	payload, err := json.Marshal(resp.Result)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}

	raw := models.RawFOBSMessage{
		Exchange:    "bybit",
		Symbol:      symbol,
		Market:      "future-orderbook-snapshot",
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}

	if r.channels.SendRaw(r.ctx, raw) {
		log.Info("orderbook data sent to raw channel")
		logger.LogDataFlowEntry(log, "bybit_api", "raw_channel", len(payload), "orderbook_entries")
		logger.IncrementSnapshotRead(len(payload))
	} else if r.ctx.Err() != nil {
		return
	} else {
		log.Warn("raw channel is full, dropping data")
	}
}

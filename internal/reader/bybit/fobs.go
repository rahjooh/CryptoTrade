package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobs "cryptoflow/internal/channel/fobs"
	metrics "cryptoflow/internal/metrics"
	bybitmetrics "cryptoflow/internal/metrics/bybit"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// Bybit_FOBS_Reader fetches futures order book snapshots from Bybit.
type Bybit_FOBS_Reader struct {
	config   *appconfig.Config
	client   *http.Client
	channels *fobs.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	ip       string
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

	r := &Bybit_FOBS_Reader{
		config:   cfg,
		client:   httpClient,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      log,
		symbols:  symbols,
		ip:       localIP,
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

	reqURL := fmt.Sprintf("%s?category=linear&symbol=%s&limit=%d", snapshotCfg.URL, symbol, snapshotCfg.Limit)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		log.WithError(err).Warn("failed to build request")
		return
	}
	req = req.WithContext(r.ctx)
	resp, err := r.client.Do(req)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook")
		return
	}
	defer resp.Body.Close()

	if metrics.IsFeatureEnabled(metrics.FeatureUsedWeight) {
		bybitmetrics.ReportUsage(r.log, resp, "bybit_reader", symbol, "future-orderbook-snapshot", r.ip)
	}

	var body struct {
		Result models.BybitFOBSresp `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		log.WithError(err).Warn("failed to decode orderbook")
		return
	}

	payload, err := json.Marshal(body.Result)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}

	if len(body.Result.Bids) == 0 && len(body.Result.Asks) == 0 {
		log.Warn("received empty orderbook from Bybit")
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
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
			"entries":       len(body.Result.Bids) + len(body.Result.Asks),
		}).Info("orderbook data sent to raw channel")
	} else if err := r.ctx.Err(); err != nil {
		log.WithError(err).Warn("failed to send orderbook to raw channel")
	} else {
		metrics.EmitDropMetric(r.log, metrics.DropMetricSnapshotRaw, "bybit", raw.Market, symbol, "raw")
		log.Warn("raw channel is full, dropping data")
	}
}

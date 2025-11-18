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
	"cryptoflow/internal/channel/foi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// Bybit_FOI_Reader periodically polls Bybit's REST open-interest endpoint
// and forwards the raw payload into foi.raw.
// IP sharding is supported by binding the HTTP client to a specific local IP
// (pass different localIP values when constructing readers on multiple hosts / NICs).
//
// Endpoint:
//
//	GET /v5/market/open-interest
//	  ?category=linear
//	  ?symbol=BTCUSDT
//	  ?intervalTime=5min
//	  ?limit=200
type Bybit_FOI_Reader struct {
	config   *appconfig.Config
	client   *http.Client
	channels *foi.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	ip       string
}

// Bybit_FOI_NewReader constructs a Bybit FOI reader.
//
//   - cfg:     global application config
//   - ch:      FOI channels (foi.raw / foi.norm)
//   - symbols: optional explicit list of symbols; if empty, config symbols are used
//   - localIP: optional IP address for outbound connections (used for IP sharding)
func Bybit_FOI_NewReader(cfg *appconfig.Config, ch *foi.Channels, symbols []string, localIP string) *Bybit_FOI_Reader {
	log := logger.GetLogger()

	pool := cfg.Source.Bybit.ConnectionPool
	transport := &http.Transport{
		MaxIdleConns:        pool.MaxIdleConns,
		MaxIdleConnsPerHost: pool.MaxIdleConns,
		MaxConnsPerHost:     pool.MaxConnsPerHost,
		IdleConnTimeout:     pool.IdleConnTimeout,
		DisableCompression:  false,
	}

	// If localIP is provided, bind the dialer to that IP. This allows
	// sharding request traffic across multiple egress IPs when running
	// multiple instances of the reader.
	if localIP != "" {
		if ip := net.ParseIP(localIP); ip != nil {
			dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}
			transport.DialContext = dialer.DialContext
		}
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.Reader.Timeout,
	}

	reader := &Bybit_FOI_Reader{
		config:   cfg,
		client:   httpClient,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      log,
		symbols:  symbols,
		ip:       localIP,
	}

	log.WithComponent("bybit_FOI_reader").WithFields(logger.Fields{
		"max_idle_conns":     pool.MaxIdleConns,
		"max_conns_per_host": pool.MaxConnsPerHost,
		"timeout":            cfg.Reader.Timeout,
	}).Info("bybit FOI reader initialized")

	return reader
}

// Bybit_FOFI_Start launches a FOI polling worker per symbol.
// It respects:
//   - source.bybit.future.open_interest.enabled
//   - connection = "rest"
//   - interval_ms (defaults to 5 minutes if zero)
func (r *Bybit_FOI_Reader) Bybit_FOI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit FOI reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	log := r.log.WithComponent("bybit_FOI_reader").WithFields(logger.Fields{
		"operation": "Bybit_FOI_Start",
	})

	foiCfg := r.config.Source.Bybit.Future.OpenInterest
	if !foiCfg.Enabled {
		log.Warn("bybit futures open_interest is disabled")
		return fmt.Errorf("bybit futures open_interest is disabled")
	}
	if foiCfg.Connection != "rest" {
		log.WithFields(logger.Fields{
			"connection": foiCfg.Connection,
		}).Warn("bybit FOI reader expects connection=rest")
		return fmt.Errorf("bybit FOI reader expects connection=rest, got %s", foiCfg.Connection)
	}

	// If no symbols passed explicitly, use config symbols.
	if len(r.symbols) == 0 {
		r.symbols = foiCfg.Symbols
	}
	if len(r.symbols) == 0 {
		log.Warn("no symbols configured for bybit FOI reader")
		return fmt.Errorf("no symbols configured for bybit FOI reader")
	}

	interval := time.Duration(foiCfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		// 5 minutes default
		interval = 5 * time.Minute
	}

	log.WithFields(logger.Fields{
		"symbols":  r.symbols,
		"interval": interval,
		"url":      foiCfg.URL,
	}).Info("starting bybit FOI reader")

	for _, symbol := range r.symbols {
		sym := symbol
		r.wg.Add(1)
		go r.fetchFOIWorker(sym, foiCfg, interval)
	}

	log.Info("bybit FOI reader started successfully")
	return nil
}

// Bybit_FOI_Stop signals all workers to stop and waits for completion.
func (r *Bybit_FOI_Reader) Bybit_FOI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("bybit_FOI_reader").Info("stopping bybit FOI reader")
	r.wg.Wait()
	r.log.WithComponent("bybit_FOI_reader").Info("bybit FOI reader stopped")
}

// fetchFOIWorker implements a time-aligned polling loop for a single symbol.
// It aligns to interval boundaries similarly to your Binance FOBS reader.
func (r *Bybit_FOI_Reader) fetchFOIWorker(symbol string, foiCfg appconfig.OpenInterestConfig, interval time.Duration) {
	defer r.wg.Done()

	log := r.log.WithComponent("bybit_FOI_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "foi_fetcher",
	})
	log.Info("starting Bybit FOI worker")

	// Align the first tick to the next interval boundary.
	now := time.Now()
	nextTick := now.Truncate(interval).Add(interval)
	timer := time.NewTimer(nextTick.Sub(now))
	defer timer.Stop()

	for {
		select {
		case <-r.ctx.Done():
			log.Info("FOI worker stopped due to context cancellation")
			return
		case <-timer.C:
			start := time.Now()
			r.fetchFOI(symbol, foiCfg)
			duration := time.Since(start)

			if duration > interval {
				log.WithFields(logger.Fields{
					"duration": duration.Milliseconds(),
					"interval": interval.Milliseconds(),
				}).Warn("FOI fetch took longer than interval")
			}

			nextTick = start.Truncate(interval).Add(interval)
			timer.Reset(time.Until(nextTick))
		}
	}
}

// fetchFOI executes a single REST call to Bybit open-interest endpoint
// and forwards the raw JSON into foi.raw for further processing.
func (r *Bybit_FOI_Reader) fetchFOI(symbol string, foiCfg appconfig.OpenInterestConfig) {
	log := r.log.WithComponent("bybit_FOI_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_foi",
	})

	market := "future-openinterest"

	baseURL := foiCfg.URL
	if baseURL == "" {
		baseURL = "https://api.bybit.com/v5/market/open-interest"
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		log.WithError(err).Warn("failed to parse FOI URL")
		return
	}
	q := u.Query()
	category := foiCfg.Category
	if category == "" {
		category = "linear"
	}
	q.Set("category", category)
	q.Set("symbol", symbol)
	// 5min interval as requested; you can make this configurable later if needed.
	q.Set("intervalTime", "5min")
	q.Set("limit", "200")
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		log.WithError(err).Warn("failed to build FOI request")
		return
	}
	req = req.WithContext(r.ctx)

	start := time.Now()
	resp, err := r.client.Do(req)
	if err != nil {
		log.WithError(err).Warn("failed to fetch FOI")
		return
	}
	duration := time.Since(start)
	defer resp.Body.Close()

	log.WithFields(logger.Fields{
		"symbol":         symbol,
		"duration_ms":    duration.Milliseconds(),
		"http_status":    resp.StatusCode,
		"content_length": resp.ContentLength,
	}).Info("FOI snapshot fetched")

	// Optional: generic HTTP weight logging hook, if your metrics package supports it.
	// If you don't have such helper, you can remove this.
	if metrics.IsFeatureEnabled(metrics.FeatureUsedWeight) {
		//metrics.ReportHTTPWeight(r.log, resp, "bybit_fofi_reader", symbol, market, r.ip)
	}

	var bybitResp models.BybitFOIOpenInterestResponse
	if err := json.NewDecoder(resp.Body).Decode(&bybitResp); err != nil {
		log.WithError(err).Warn("failed to decode Bybit FOI response")
		return
	}

	payload, err := json.Marshal(bybitResp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal Bybit FOI response")
		return
	}

	raw := models.RawFOI{
		Exchange: models.ExchangeBybit,
		Payload:  payload,
	}

	if r.channels.SendRaw(r.ctx, raw) {
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
		}).Info("Bybit FOI data sent to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "bybit", market, symbol, "raw")
		log.Warn("FOI raw channel is full, dropping Bybit FOI data")
	}
}

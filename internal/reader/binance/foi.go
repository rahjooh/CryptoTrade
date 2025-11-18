package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"cryptoflow/config"
	appconfig "cryptoflow/config"
	"cryptoflow/internal/channel/foi"
	"cryptoflow/internal/metrics"
	binancemetrics "cryptoflow/internal/metrics/binance"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/sirupsen/logrus"
)

type Binance_FOI_Reader struct {
	config   *config.Config
	client   *http.Client
	channels *foi.Channels
	ctx      context.Context
	symbols  []string
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	interval time.Duration
	baseURL  string
	ip       string
}

func Binance_FOI_NewReader(cfg *appconfig.Config, ch *foi.Channels, symbols []string, localIP string) *Binance_FOI_Reader {
	log := logger.GetLogger()

	pool := cfg.Source.Binance.ConnectionPool
	transport := &http.Transport{
		MaxIdleConns:        pool.MaxIdleConns,
		MaxIdleConnsPerHost: pool.MaxIdleConns,
		MaxConnsPerHost:     pool.MaxConnsPerHost,
		IdleConnTimeout:     pool.IdleConnTimeout,
		DisableCompression:  false,
	}

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
	reader := &Binance_FOI_Reader{
		config:   cfg,
		client:   httpClient,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      log,
		symbols:  symbols,
		ip:       localIP,
	}

	log.WithComponent("binance_FOI_reader").WithFields(logger.Fields{
		"max_idle_conns":     pool.MaxIdleConns,
		"max_conns_per_host": pool.MaxConnsPerHost,
		"timeout":            cfg.Reader.Timeout,
	}).Info("binance FOI reader initialized")

	return reader
	//foiCfg := cfg.Source.Binance.Future.OpenInterest // now BinanceOpenInterestConfig
	//
	//baseURL := strings.TrimSpace(foiCfg.URL)
	//if baseURL == "" {
	//	baseURL = "https://fapi.binance.com/fapi/v1/openInterest"
	//}
	//
	//interval := time.Duration(foiCfg.IntervalMs) * time.Millisecond
	//if interval <= 0 {
	//	interval = time.Second
	//}
	//
	//return &Binance_FOI_Reader{
	//	config: cfg,
	//	client: &http.Client{
	//		Timeout: 5 * time.Second,
	//	},
	//	symbols:  symbols,
	//	interval: interval,
	//	baseURL:  baseURL,
	//}
}

// Binance_FOI_Start begins fetching open interest snapshots for configured symbols.
func (r *Binance_FOI_Reader) Binance_FOI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("binance FOI reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	log := r.log.WithComponent("binance_FOI_reader").WithFields(logger.Fields{"operation": "Binance_FOI_Start"})

	foiCfg := r.config.Source.Binance.Future.OpenInterest
	if !foiCfg.Enabled {
		log.Info("binance futures open_interest is disabled via configuration; reader will not start")
		return fmt.Errorf("binance futures open_interest is disabled")
	}
	if foiCfg.Connection != "rest" {
		log.WithFields(logger.Fields{
			"connection": foiCfg.Connection,
		}).Warn("binance FOI reader expects connection=rest")
		return fmt.Errorf("binance FOI reader expects connection=rest, got %s", foiCfg.Connection)
	}

	if len(r.symbols) == 0 {
		log.Warn("no symbols configured for binance FOI reader")
		return fmt.Errorf("no symbols configured for binance FOI reader")
	}

	interval := time.Duration(foiCfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}

	log.WithFields(logger.Fields{
		"symbols":  r.symbols,
		"interval": interval,
		"url":      foiCfg.URL,
	}).Info("starting binance FOI reader")

	for _, symbol := range r.symbols {
		sym := symbol
		r.wg.Add(1)
		go r.fetchFOIWorker(sym, foiCfg, interval)
	}

	log.Info("binance FOI reader started successfully")
	return nil
}

// Binance_FOI_Stop signals all workers to stop and waits for completion.
func (r *Binance_FOI_Reader) Binance_FOI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_FOI_reader").Info("stopping binance FOI reader")
	r.wg.Wait()
	r.log.WithComponent("binance_FOI_reader").Info("binance FOI reader stopped")
}

func (r *Binance_FOI_Reader) fetchFOIWorker(symbol string, foiCfg appconfig.BinanceOpenInterestConfig, interval time.Duration) {
	defer r.wg.Done()

	log := r.log.WithComponent("binance_FOI_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "foi_fetcher",
	})

	log.Info("starting FOI worker")

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

func (r *Binance_FOI_Reader) fetchFOI(symbol string, foiCfg appconfig.BinanceOpenInterestConfig) {
	log := r.log.WithComponent("binance_FOI_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_foi",
	})

	market := "future-openinterest"

	baseURL := foiCfg.URL
	if baseURL == "" {
		baseURL = "https://fapi.binance.com/fapi/v1/openInterest"
	}

	// Build URL with query params
	u, err := url.Parse(baseURL)
	if err != nil {
		log.WithError(err).Warn("failed to parse FOI URL")
		return
	}
	q := u.Query()
	q.Set("symbol", symbol)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		log.WithError(err).Warn("failed to build FOI request")
		return
	}
	req = req.WithContext(r.ctx)

	if r.log.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logger.Fields{
			"url":    u.String(),
			"symbol": symbol,
		}).Debug("issuing FOI HTTP request")
	}

	start := time.Now()
	resp, err := r.client.Do(req)
	if err != nil {
		log.WithError(err).Warn("failed to fetch FOI")
		return
	}
	duration := time.Since(start)
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		log.WithFields(logger.Fields{
			"http_status": resp.StatusCode,
			"symbol":      symbol,
		}).Warn("FOI endpoint returned non-success status")
	}

	log.WithFields(logger.Fields{
		"symbol":         symbol,
		"duration_ms":    duration.Milliseconds(),
		"http_status":    resp.StatusCode,
		"content_length": resp.ContentLength,
	}).Info("FOI snapshot fetched")

	if metrics.IsFeatureEnabled(metrics.FeatureUsedWeight) {
		// For FOI, we don't have a nice estimator yet; report 0 extra weight.
		binancemetrics.ReportUsedWeight(r.log, resp, "binance_FOI_reader", symbol, market, r.ip, 0)
	}

	var binanceResp models.BinanceFOICurrentResp
	if err := json.NewDecoder(resp.Body).Decode(&binanceResp); err != nil {
		log.WithError(err).Warn("failed to decode FOI response")
		return
	}

	payload, err := json.Marshal(binanceResp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal FOI response")
		return
	}

	if r.log.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logger.Fields{
			"decoded_response": binanceResp,
			"symbol":           symbol,
		}).Debug("decoded FOI payload")
	}

	raw := models.RawFOI{
		Exchange: models.ExchangeBinance,
		Payload:  payload,
	}

	if r.channels.SendRaw(r.ctx, raw) {
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
			"open_interest": binanceResp.OpenInterest,
		}).Info("FOI data sent to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		// Pick a proper drop metric name consistent with your metrics package
		metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "binance", market, symbol, "raw")
		log.Warn("FOI raw channel is full, dropping data")
	}
}

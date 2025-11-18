package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fpichannel "cryptoflow/internal/channel/fpi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// Bybit_FPI_Reader polls premium-index data from Bybit REST endpoints.
type Bybit_FPI_Reader struct {
	config   *appconfig.Config
	channels *fpichannel.Channels
	client   *http.Client
	ctx      context.Context
	mu       sync.RWMutex
	wg       *sync.WaitGroup
	log      *logger.Log

	running bool
	symbols []string
	ip      string
}

// Bybit_FPI_NewReader creates a new reader bound to the shard IP.
func Bybit_FPI_NewReader(cfg *appconfig.Config, ch *fpichannel.Channels, symbols []string, localIP string) *Bybit_FPI_Reader {
	log := logger.GetLogger()
	pool := cfg.Source.Bybit.ConnectionPool

	transport := &http.Transport{
		MaxIdleConns:        pool.MaxIdleConns,
		MaxIdleConnsPerHost: pool.MaxIdleConns,
		MaxConnsPerHost:     pool.MaxConnsPerHost,
		IdleConnTimeout:     pool.IdleConnTimeout,
	}
	if localIP != "" {
		if ip := net.ParseIP(localIP); ip != nil {
			dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}
			transport.DialContext = dialer.DialContext
		}
	}

	return &Bybit_FPI_Reader{
		config:   cfg,
		channels: ch,
		client: &http.Client{
			Timeout:   cfg.Reader.Timeout,
			Transport: transport,
		},
		log:     log,
		wg:      &sync.WaitGroup{},
		symbols: symbols,
		ip:      localIP,
	}
}

// Bybit_FPI_Start launches polling workers per symbol.
func (r *Bybit_FPI_Reader) Bybit_FPI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit FPI reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.FPI
	if !cfg.Enabled {
		return fmt.Errorf("bybit FPI disabled via configuration")
	}
	if cfg.Connection != "rest" {
		return fmt.Errorf("bybit FPI expects connection=rest, got %s", cfg.Connection)
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for bybit FPI reader")
		}
		r.symbols = cfg.Symbols
	}

	interval := time.Duration(cfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 3 * time.Second
	}

	for _, sym := range r.symbols {
		s := strings.ToUpper(strings.TrimSpace(sym))
		if s == "" {
			continue
		}
		r.wg.Add(1)
		go r.pollSymbol(s, cfg, interval)
	}

	r.log.WithComponent("bybit_fpi_reader").WithFields(logger.Fields{
		"symbols":  len(r.symbols),
		"ip":       r.ip,
		"interval": interval,
	}).Info("bybit FPI reader started")
	return nil
}

// Bybit_FPI_Stop waits for all workers to exit.
func (r *Bybit_FPI_Reader) Bybit_FPI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("bybit_fpi_reader").Info("stopping bybit FPI reader")
	r.wg.Wait()
	r.log.WithComponent("bybit_fpi_reader").Info("bybit FPI reader stopped")
}

func (r *Bybit_FPI_Reader) pollSymbol(symbol string, cfg appconfig.PremiumIndexConfig, interval time.Duration) {
	defer r.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := r.fetch(symbol, cfg); err != nil {
			r.log.WithComponent("bybit_fpi_reader").WithFields(logger.Fields{
				"symbol": symbol,
			}).WithError(err).Debug("failed to fetch bybit FPI data")
		}

		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Bybit_FPI_Reader) fetch(symbol string, cfg appconfig.PremiumIndexConfig) error {
	baseURL := cfg.URL
	if baseURL == "" {
		baseURL = "https://api.bybit.com/v5/market/tickers"
	}

	endpoint, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("invalid bybit FPI URL: %w", err)
	}
	q := endpoint.Query()
	category := strings.TrimSpace(cfg.Category)
	if category == "" {
		category = "linear"
	}
	q.Set("category", category)
	q.Set("symbol", symbol)
	endpoint.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("bybit FPI request failed: %s", resp.Status)
	}

	var payload bybitFpiResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	if payload.RetCode != 0 {
		return fmt.Errorf("bybit FPI returned retCode=%d message=%s", payload.RetCode, payload.RetMsg)
	}
	if payload.Result.Category != "" && strings.ToLower(payload.Result.Category) != strings.ToLower(category) {
		// still continue but log diff
	}
	if len(payload.Result.List) == 0 {
		return nil
	}

	eventTime := time.UnixMilli(payload.Time).UTC()
	for _, entry := range payload.Result.List {
		mark := parseFloat(entry.MarkPrice)
		index := parseFloat(entry.IndexPrice)
		funding := parseFloat(entry.FundingRate)
		nextFunding := parseTimeMillis(entry.NextFundingTime)

		msg := models.RawFPI{
			Exchange:        models.ExchangeBybit,
			Market:          "fpi",
			Symbol:          strings.ToUpper(entry.Symbol),
			MarkPrice:       mark,
			IndexPrice:      index,
			FundingRate:     funding,
			NextFundingTime: nextFunding,
			PremiumIndex:    mark - index,
			EventTime:       eventTime,
			Source:          "bybit_rest",
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, models.ExchangeBybit, "fpi", msg.Symbol, "raw")
		}
	}
	return nil
}

type bybitFpiResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string `json:"category"`
		List     []struct {
			Symbol          string `json:"symbol"`
			LastPrice       string `json:"lastPrice"`
			IndexPrice      string `json:"indexPrice"`
			MarkPrice       string `json:"markPrice"`
			FundingRate     string `json:"fundingRate"`
			NextFundingTime string `json:"nextFundingTime"`
		} `json:"list"`
	} `json:"result"`
	Time int64 `json:"time"`
}

func parseTimeMillis(ts string) time.Time {
	if ts == "" {
		return time.Time{}
	}
	val, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(val).UTC()
}

func parseFloat(v string) float64 {
	if v == "" {
		return 0
	}
	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return val
}

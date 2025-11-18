package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	"golang.org/x/time/rate"
)

// Kucoin_FPI_Reader polls premium-index data from KuCoin REST endpoints.
type Kucoin_FPI_Reader struct {
	config   *appconfig.Config
	channels *fpichannel.Channels
	client   *http.Client
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	log      *logger.Log

	running  bool
	symbols  []string
	limiter  *rate.Limiter
	interval time.Duration
}

// Kucoin_FPI_NewReader creates a new reader instance.
func Kucoin_FPI_NewReader(cfg *appconfig.Config, ch *fpichannel.Channels, symbols []string) *Kucoin_FPI_Reader {
	rl := cfg.Reader.RateLimit
	rps := rl.RequestsPerSecond
	if rps <= 0 {
		rps = 5
	}
	burst := rl.BurstSize
	if burst <= 0 {
		burst = 1
	}

	return &Kucoin_FPI_Reader{
		config:   cfg,
		channels: ch,
		client: &http.Client{
			Timeout: cfg.Reader.Timeout,
		},
		wg:      &sync.WaitGroup{},
		log:     logger.GetLogger(),
		symbols: symbols,
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// Kucoin_FPI_Start schedules polling workers.
func (r *Kucoin_FPI_Reader) Kucoin_FPI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("kucoin FPI reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.FPI
	if !cfg.Enabled {
		return fmt.Errorf("kucoin FPI disabled via configuration")
	}
	if cfg.Connection != "rest" {
		return fmt.Errorf("kucoin FPI expects connection=rest")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for kucoin FPI reader")
		}
		r.symbols = cfg.Symbols
	}

	interval := time.Duration(cfg.IntervalMs) * time.Millisecond
	if interval <= 0 && cfg.StreamInterval > 0 {
		interval = cfg.StreamInterval
	}
	if interval <= 0 {
		interval = time.Second * 5
	}
	r.interval = interval

	for _, sym := range r.symbols {
		s := strings.ToUpper(strings.TrimSpace(sym))
		if s == "" {
			continue
		}
		r.wg.Add(1)
		go r.pollSymbol(s, cfg)
	}

	r.log.WithComponent("kucoin_fpi_reader").WithFields(logger.Fields{
		"symbols":  len(r.symbols),
		"interval": interval,
	}).Info("kucoin FPI reader started")
	return nil
}

// Kucoin_FPI_Stop waits for workers to finish.
func (r *Kucoin_FPI_Reader) Kucoin_FPI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_fpi_reader").Info("stopping kucoin FPI reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_fpi_reader").Info("kucoin FPI reader stopped")
}

func (r *Kucoin_FPI_Reader) pollSymbol(symbol string, cfg appconfig.PremiumIndexConfig) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.fetch(symbol, cfg); err != nil {
			r.log.WithComponent("kucoin_fpi_reader").WithFields(logger.Fields{
				"symbol": symbol,
			}).WithError(err).Debug("failed to fetch kucoin FPI data")
		}

		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Kucoin_FPI_Reader) fetch(symbol string, cfg appconfig.PremiumIndexConfig) error {
	if err := r.limiter.Wait(r.ctx); err != nil {
		return err
	}

	base := strings.TrimSpace(cfg.URL)
	if base == "" {
		base = "https://api-futures.kucoin.com/api/v1/mark-price"
	}

	endpoint, err := url.Parse(base)
	if err != nil {
		return fmt.Errorf("invalid kucoin FPI url: %w", err)
	}
	if strings.Contains(endpoint.Path, "{symbol}") {
		endpoint.Path = strings.ReplaceAll(endpoint.Path, "{symbol}", symbol)
	} else if !strings.HasSuffix(endpoint.Path, symbol+"/current") {
		if !strings.HasSuffix(endpoint.Path, "/") {
			endpoint.Path += "/"
		}
		endpoint.Path = endpoint.Path + symbol + "/current"
	}

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
		return fmt.Errorf("kucoin FPI request failed: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var payload kucoinFpiResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	if payload.Code != "" && payload.Code != "200000" {
		return fmt.Errorf("kucoin FPI returned code=%s", payload.Code)
	}

	eventTime := time.Now().UTC()
	switch {
	case payload.Data.Timestamp > 0:
		eventTime = time.UnixMilli(payload.Data.Timestamp).UTC()
	case payload.Data.TimePoint > 0:
		eventTime = time.UnixMilli(payload.Data.TimePoint).UTC()
	}

	mark := payload.Data.MarkPrice
	index := payload.Data.IndexPrice
	markFloat := parseFloat(mark)
	indexFloat := parseFloat(index)
	funding := parseFloat(payload.Data.FundingRate)

	msg := models.RawFPI{
		Exchange:     models.ExchangeKucoin,
		Market:       "fpi",
		Symbol:       symbol,
		MarkPrice:    markFloat,
		IndexPrice:   indexFloat,
		FundingRate:  funding,
		PremiumIndex: markFloat - indexFloat,
		EventTime:    eventTime,
		Source:       "kucoin_rest",
		Payload:      append([]byte(nil), body...),
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, models.ExchangeKucoin, "fpi", symbol, "raw")
	}
	return nil
}

type kucoinFpiResponse struct {
	Code string `json:"code"`
	Data struct {
		Symbol      string `json:"symbol"`
		MarkPrice   string `json:"markPrice"`
		IndexPrice  string `json:"indexPrice"`
		FundingRate string `json:"fundingRate"`
		Timestamp   int64  `json:"time"`
		TimePoint   int64  `json:"timePoint"`
	} `json:"data"`
}

func parseFloat(v string) float64 {
	if v == "" {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return f
}

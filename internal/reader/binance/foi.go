package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	foichannel "cryptoflow/internal/channel/foi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"golang.org/x/time/rate"
)

// Binance_FOI_Reader polls Binance futures open-interest REST endpoints for configured symbols.
type Binance_FOI_Reader struct {
	config   *appconfig.Config
	channels *foichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	client   *http.Client
	limiter  *rate.Limiter
	interval time.Duration
	baseURL  string
}

// Binance_FOI_NewReader creates a new FOI reader.
func Binance_FOI_NewReader(cfg *appconfig.Config, ch *foichannel.Channels, symbols []string, _ string) *Binance_FOI_Reader {
	return &Binance_FOI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		client: &http.Client{
			Timeout: cfg.Reader.Timeout,
		},
	}
}

// Binance_FOI_Start begins polling open-interest for all configured symbols.
func (r *Binance_FOI_Reader) Binance_FOI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("binance open-interest reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.OpenInterest
	if !cfg.Enabled {
		return fmt.Errorf("binance futures open interest disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for binance open-interest reader")
		}
		r.symbols = cfg.Symbols
	}

	interval := time.Duration(cfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Duration(cfg.IntervalMinutes) * time.Minute
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	r.interval = interval

	rateLimit := cfg.StreamInterval
	if rateLimit <= 0 {
		rateLimit = 250 * time.Millisecond
	}
	r.limiter = rate.NewLimiter(rate.Every(rateLimit), 1)

	baseURL := strings.TrimSpace(cfg.URL)
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	r.baseURL = strings.TrimRight(baseURL, "/")

	for _, sym := range r.symbols {
		symbol := strings.ToUpper(strings.TrimSpace(sym))
		if symbol == "" {
			continue
		}
		r.wg.Add(1)
		go r.pollSymbol(symbol)
	}

	r.log.WithComponent("binance_foi_reader").WithFields(logger.Fields{
		"symbols":  strings.Join(r.symbols, ","),
		"interval": interval.String(),
	}).Info("binance FOI reader started")
	return nil
}

// Binance_FOI_Stop stops polling goroutines.
func (r *Binance_FOI_Reader) Binance_FOI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_foi_reader").Info("stopping binance FOI reader")
	r.wg.Wait()
	r.log.WithComponent("binance_foi_reader").Info("binance FOI reader stopped")
}

func (r *Binance_FOI_Reader) pollSymbol(symbol string) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.fetchOnce(symbol); err != nil {
			r.log.WithComponent("binance_foi_reader").WithFields(logger.Fields{
				"symbol": symbol,
			}).WithError(err).Debug("open-interest request failed")
		}

		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Binance_FOI_Reader) fetchOnce(symbol string) error {
	if err := r.limiter.Wait(r.ctx); err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/fapi/v1/openInterest?symbol=%s", r.baseURL, symbol)
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	var payload models.BinanceFOICurrentResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("decode binance foi response: %w", err)
	}
	payload.Symbol = symbol

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal foi payload: %w", err)
	}

	msg := models.RawFOIMessage{
		Exchange:  "binance",
		Symbol:    symbol,
		Market:    "future-openinterest",
		Data:      rawPayload,
		Timestamp: time.Now().UTC(),
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "binance", "future-openinterest", symbol, "raw")
	}
	return nil
}

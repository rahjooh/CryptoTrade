package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	oichannel "cryptoflow/internal/channel/oi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	sdkapi "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/api"
	futuresmarket "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/generate/futures/market"
	sdktype "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/types"
	"golang.org/x/time/rate"
)

// Kucoin_OI_Reader polls KuCoin futures open-interest via the REST API until a websocket feed becomes available.
type Kucoin_OI_Reader struct {
	config    *appconfig.Config
	marketAPI futuresmarket.MarketAPI
	channels  *oichannel.Channels
	ctx       context.Context
	wg        *sync.WaitGroup
	mu        sync.RWMutex
	running   bool
	log       *logger.Log
	symbols   []string
	limiter   *rate.Limiter
	interval  time.Duration
}

// Kucoin_OI_NewReader initialises the REST client backed reader.
func Kucoin_OI_NewReader(cfg *appconfig.Config, ch *oichannel.Channels, symbols []string) *Kucoin_OI_Reader {
	log := logger.GetLogger()
	openCfg := cfg.Source.Kucoin.Future.OpenInterest

	baseURL := openCfg.URL
	if baseURL == "" {
		baseURL = "https://api-futures.kucoin.com"
	} else if u, err := url.Parse(openCfg.URL); err == nil {
		baseURL = fmt.Sprintf("https://%s", u.Host)
	}

	transportOpt := sdktype.NewTransportOptionBuilder().
		SetMaxIdleConns(cfg.Source.Kucoin.ConnectionPool.MaxIdleConns).
		SetMaxIdleConnsPerHost(cfg.Source.Kucoin.ConnectionPool.MaxIdleConns).
		SetMaxConnsPerHost(cfg.Source.Kucoin.ConnectionPool.MaxConnsPerHost).
		SetIdleConnTimeout(cfg.Source.Kucoin.ConnectionPool.IdleConnTimeout).
		SetTimeout(cfg.Reader.Timeout).
		Build()

	option := sdktype.NewClientOptionBuilder().
		WithFuturesEndpoint(baseURL).
		WithTransportOption(transportOpt).
		Build()

	client := sdkapi.NewClient(option)
	marketAPI := client.RestService().GetFuturesService().GetMarketAPI()

	rl := cfg.Reader.RateLimit
	rps := rl.RequestsPerSecond
	if rps <= 0 {
		rps = 5
	}
	burst := rl.BurstSize
	if burst <= 0 {
		burst = 1
	}

	return &Kucoin_OI_Reader{
		config:    cfg,
		marketAPI: marketAPI,
		channels:  ch,
		wg:        &sync.WaitGroup{},
		log:       log,
		symbols:   symbols,
		limiter:   rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// Kucoin_OI_Start begins the polling loops per symbol.
func (r *Kucoin_OI_Reader) Kucoin_OI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("kucoin open-interest reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.OpenInterest
	if !cfg.Enabled {
		return fmt.Errorf("kucoin open-interest disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for kucoin open-interest reader")
		}
		r.symbols = cfg.Symbols
	}

	interval := time.Duration(cfg.IntervalMinutes) * time.Minute
	if cfg.IntervalMs > 0 {
		interval = time.Duration(cfg.IntervalMs) * time.Millisecond
	}
	if interval <= 0 {
		interval = time.Minute
	}
	r.interval = interval

	for _, symbol := range r.symbols {
		sym := strings.ToUpper(symbol)
		r.wg.Add(1)
		go r.pollSymbol(sym)
	}

	r.log.WithComponent("kucoin_oi_reader").WithFields(logger.Fields{
		"symbols":  r.symbols,
		"interval": interval.String(),
	}).Info("kucoin open-interest reader started")
	return nil
}

// Kucoin_OI_Stop cancels all polling goroutines.
func (r *Kucoin_OI_Reader) Kucoin_OI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_oi_reader").Info("stopping kucoin open-interest reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_oi_reader").Info("kucoin open-interest reader stopped")
}

func (r *Kucoin_OI_Reader) pollSymbol(symbol string) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.fetchOnce(symbol); err != nil {
			r.log.WithComponent("kucoin_oi_reader").WithFields(logger.Fields{
				"symbol": symbol,
			}).WithError(err).Debug("failed to fetch kucoin open-interest")
		}

		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Kucoin_OI_Reader) fetchOnce(symbol string) error {
	if err := r.limiter.Wait(r.ctx); err != nil {
		return err
	}

	req := futuresmarket.NewGetSymbolReqBuilder().SetSymbol(symbol).Build()
	resp, err := r.marketAPI.GetSymbol(req, r.ctx)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("empty response for symbol %s", symbol)
	}

	value, _ := strconv.ParseFloat(resp.OpenInterest, 64)

	payload := map[string]any{
		"symbol":       resp.Symbol,
		"openInterest": resp.OpenInterest,
		"time":         time.Now().UTC().UnixMilli(),
	}
	raw, _ := json.Marshal(payload)

	msg := models.RawOIMessage{
		Exchange:  "kucoin",
		Symbol:    strings.ToUpper(symbol),
		Market:    "oi",
		Value:     value,
		Source:    "kucoin_rest",
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "kucoin", "oi", symbol, "raw")
	}
	return nil
}

package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	pichannel "cryptoflow/internal/channel/pi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	api "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/api"
	futuresmarket "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/generate/futures/market"
	sdktype "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/types"
	"golang.org/x/time/rate"
)

// Kucoin_PI_Reader polls premium index data from KuCoin REST API.
type Kucoin_PI_Reader struct {
	config    *appconfig.Config
	channels  *pichannel.Channels
	marketAPI futuresmarket.MarketAPI

	ctx context.Context
	wg  *sync.WaitGroup
	mu  sync.RWMutex

	log      *logger.Log
	running  bool
	symbols  []string
	limiter  *rate.Limiter
	interval time.Duration
}

// Kucoin_PI_NewReader creates a new premium-index reader.
func Kucoin_PI_NewReader(cfg *appconfig.Config, ch *pichannel.Channels, symbols []string) *Kucoin_PI_Reader {
	log := logger.GetLogger()
	cfgPI := cfg.Source.Kucoin.Future.PremiumIndex

	baseURL := cfgPI.URL
	if baseURL == "" {
		baseURL = "https://api-futures.kucoin.com"
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

	client := api.NewClient(option)
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

	return &Kucoin_PI_Reader{
		config:    cfg,
		channels:  ch,
		marketAPI: marketAPI,
		wg:        &sync.WaitGroup{},
		log:       log,
		symbols:   symbols,
		limiter:   rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// Kucoin_PI_Start schedules polling loops per symbol.
func (r *Kucoin_PI_Reader) Kucoin_PI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("kucoin premium-index reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.PremiumIndex
	if !cfg.Enabled {
		return fmt.Errorf("kucoin premium-index disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for kucoin premium-index reader")
		}
		r.symbols = cfg.Symbols
	}

	interval := time.Duration(cfg.IntervalMinutes) * time.Minute
	if cfg.IntervalMs > 0 {
		interval = time.Duration(cfg.IntervalMs) * time.Millisecond
	}
	if cfg.StreamInterval > 0 {
		interval = cfg.StreamInterval
	}
	if interval <= 0 {
		interval = time.Minute
	}
	r.interval = interval

	for _, symbol := range r.symbols {
		s := strings.ToUpper(symbol)
		r.wg.Add(1)
		go r.pollSymbol(s)
	}

	r.log.WithComponent("kucoin_pi_reader").WithFields(logger.Fields{
		"symbols":  r.symbols,
		"interval": interval.String(),
	}).Info("kucoin premium-index reader started")
	return nil
}

// Kucoin_PI_Stop waits for all polling goroutines to finish.
func (r *Kucoin_PI_Reader) Kucoin_PI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_pi_reader").Info("stopping kucoin premium-index reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_pi_reader").Info("kucoin premium-index reader stopped")
}

func (r *Kucoin_PI_Reader) pollSymbol(symbol string) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.fetchOnce(symbol); err != nil {
			r.log.WithComponent("kucoin_pi_reader").WithFields(logger.Fields{
				"symbol": symbol,
			}).WithError(err).Debug("failed to fetch kucoin premium index")
		}

		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Kucoin_PI_Reader) fetchOnce(symbol string) error {
	if err := r.limiter.Wait(r.ctx); err != nil {
		return err
	}

	reqBuilder := futuresmarket.NewGetPremiumIndexReqBuilder().SetSymbol(symbol).SetMaxCount(1)
	req := reqBuilder.Build()
	resp, err := r.marketAPI.GetPremiumIndex(req, r.ctx)
	if err != nil {
		return err
	}
	if resp == nil || len(resp.DataList) == 0 {
		return nil
	}

	entry := resp.DataList[0]
	ts := time.UnixMilli(entry.TimePoint).UTC()
	payload, _ := json.Marshal(entry)

	msg := models.RawPIMessage{
		Exchange:     "kucoin",
		Symbol:       symbol,
		Market:       "pi",
		PremiumIndex: entry.Value,
		Source:       "kucoin_rest",
		Timestamp:    ts,
		Payload:      payload,
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, "kucoin", "pi", symbol, "raw")
	}
	return nil
}

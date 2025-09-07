package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"

	sdkapi "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/api"
	futuresmarket "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/generate/futures/market"
	sdktype "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/types"
	"golang.org/x/time/rate"
)

// Kucoin_FOBS_Reader fetches futures order book snapshots from KuCoin.
type Kucoin_FOBS_Reader struct {
	config     *config.Config
	marketAPI  futuresmarket.MarketAPI
	rawChannel chan<- models.RawFOBSMessage
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log
	symbols    []string
	limiter    *rate.Limiter
}

// Kucoin_FOBS_NewReader creates a new Kucoin_FOBS_Reader using the KuCoin universal SDK.
// The reader will fetch snapshots only for the supplied symbols. The localIP parameter is kept
// for compatibility but not used by the underlying SDK.
func Kucoin_FOBS_NewReader(cfg *config.Config, rawChannel chan<- models.RawFOBSMessage, symbols []string, localIP string) *Kucoin_FOBS_Reader {
	log := logger.GetLogger()

	snapshotCfg := cfg.Source.Kucoin.Future.Orderbook.Snapshots

	baseURL := snapshotCfg.URL
	if u, err := url.Parse(snapshotCfg.URL); err == nil {
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

	var limiter *rate.Limiter
	rl := cfg.Reader.RateLimit
	if rl.RequestsPerSecond > 0 {
		burst := rl.BurstSize
		if burst <= 0 {
			burst = rl.RequestsPerSecond
		}
		limiter = rate.NewLimiter(rate.Limit(rl.RequestsPerSecond), burst)
	}

	reader := &Kucoin_FOBS_Reader{
		config:     cfg,
		marketAPI:  marketAPI,
		rawChannel: rawChannel,
		wg:         &sync.WaitGroup{},
		log:        log,
		symbols:    symbols,
		limiter:    limiter,
	}

	log.WithComponent("kucoin_reader").WithFields(logger.Fields{
		"base_url": baseURL,
	}).Info("kucoin reader initialized")

	return reader
}

// Kucoin_FOBS_Start begins fetching order book snapshots for configured symbols.
func (r *Kucoin_FOBS_Reader) Kucoin_FOBS_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	log := r.log.WithComponent("kucoin_reader").WithFields(logger.Fields{"operation": "Kucoin_FOBS_Start"})

	snapshotCfg := r.config.Source.Kucoin.Future.Orderbook.Snapshots
	if !snapshotCfg.Enabled {
		log.Warn("kucoin futures orderbook snapshots are disabled")
		return fmt.Errorf("kucoin futures orderbook snapshots are disabled")
	}

	log.WithFields(logger.Fields{
		"symbols":  r.symbols,
		"interval": snapshotCfg.IntervalMs,
	}).Info("starting kucoin reader")

	for _, symbol := range r.symbols {
		r.wg.Add(1)
		go r.Kucoin_FOBS_FetchWorker(symbol, snapshotCfg)
	}

	log.Info("kucoin reader started successfully")
	return nil
}

// Kucoin_FOBS_Stop signals all workers to stop and waits for completion.
func (r *Kucoin_FOBS_Reader) Kucoin_FOBS_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_reader").Info("stopping kucoin reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_reader").Info("kucoin reader stopped")
}

func (r *Kucoin_FOBS_Reader) Kucoin_FOBS_FetchWorker(symbol string, snapshotCfg config.KucoinSnapshotConfig) {
	defer r.wg.Done()

	log := r.log.WithComponent("kucoin_reader").WithFields(logger.Fields{
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
		case <-r.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case <-timer.C:
			start := time.Now()
			r.Kucoin_FOBS_Fetcher(symbol)
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

func (r *Kucoin_FOBS_Reader) Kucoin_FOBS_Fetcher(symbol string) {
	log := r.log.WithComponent("kucoin_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_orderbook",
	})

	req := futuresmarket.NewGetFullOrderBookReqBuilder().SetSymbol(symbol).Build()

	retryCfg := r.config.Reader.Retry
	maxAttempts := retryCfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	delay := retryCfg.BaseDelay
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}
	maxDelay := retryCfg.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}
	multiplier := retryCfg.BackoffMultiplier
	if multiplier <= 0 {
		multiplier = 2
	}

	var resp *futuresmarket.GetFullOrderBookResp
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if r.limiter != nil {
			if err = r.limiter.Wait(r.ctx); err != nil {
				log.WithError(err).Warn("rate limiter wait failed")
				return
			}
		}

		resp, err = r.marketAPI.GetFullOrderBook(req, r.ctx)
		if err == nil {
			break
		}

		if attempt == maxAttempts {
			log.WithError(err).Warn("failed to fetch orderbook")
			return
		}

		log.WithError(err).WithField("attempt", attempt).Warnf("retrying in %v", delay)

		select {
		case <-time.After(delay):
		case <-r.ctx.Done():
			return
		}

		delay = delay * time.Duration(multiplier)
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	if resp == nil {
		log.Warn("received nil response from kucoin")
		return
	}

	bids := make([][]string, len(resp.Bids))
	for i, b := range resp.Bids {
		if len(b) < 2 {
			continue
		}
		bids[i] = []string{
			strconv.FormatFloat(b[0], 'f', -1, 64),
			strconv.FormatFloat(b[1], 'f', -1, 64),
		}
	}

	asks := make([][]string, len(resp.Asks))
	for i, a := range resp.Asks {
		if len(a) < 2 {
			continue
		}
		asks[i] = []string{
			strconv.FormatFloat(a[0], 'f', -1, 64),
			strconv.FormatFloat(a[1], 'f', -1, 64),
		}
	}

	kucoinResp := models.BinanceFOBSresp{
		LastUpdateID: resp.Sequence,
		Bids:         bids,
		Asks:         asks,
	}

	payload, err := json.Marshal(kucoinResp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}

	rawData := models.RawFOBSMessage{
		Exchange:    "kucoin",
		Symbol:      symbols.ToBinance("kucoin", symbol),
		Market:      "future-orderbook-snapshot",
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}

	select {
	case r.rawChannel <- rawData:
		log.Info("orderbook data sent to raw channel")
		logger.LogDataFlowEntry(log, "kucoin_api", "raw_channel", len(asks)+len(bids), "orderbook_entries")
		logger.IncrementSnapshotRead(len(payload))
	case <-r.ctx.Done():
		return
	default:
		log.Warn("raw channel is full, dropping data")
	}
}

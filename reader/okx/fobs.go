package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"

	okex "github.com/tfxq/okx-go-sdk"
	"github.com/tfxq/okx-go-sdk/api/rest"
	marketres "github.com/tfxq/okx-go-sdk/responses/market"
	"golang.org/x/time/rate"
)

// Okx_FOBS_Reader fetches futures order book snapshots from OKX.
type Okx_FOBS_Reader struct {
	config     *config.Config
	client     *rest.ClientRest
	rawChannel chan<- models.RawFOBSMessage
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log
	symbols    []string
	limiter    *rate.Limiter
}

// Okx_FOBS_NewReader creates a new OKX snapshot reader. The reader
// will only fetch snapshots for the provided symbols.
func Okx_FOBS_NewReader(cfg *config.Config, rawChannel chan<- models.RawFOBSMessage, symbols []string, localIP string) *Okx_FOBS_Reader {
	// SDK does not allow custom HTTP client easily, rely on defaults.
	base := okex.RestURL
	if parsed, err := url.Parse(cfg.Source.Okx.Future.Orderbook.Snapshots.URL); err == nil {
		base = okex.BaseURL(fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host))
	}
	client := rest.NewClient("", "", "", base, okex.NormalServer)

	rl := cfg.Reader.RateLimit
	rps := rl.RequestsPerSecond
	if rps <= 0 {
		rps = 5
	}
	burst := rl.BurstSize
	if burst <= 0 {
		burst = 1
	}
	limiter := rate.NewLimiter(rate.Limit(rps), burst)

	return &Okx_FOBS_Reader{
		config:     cfg,
		client:     client,
		rawChannel: rawChannel,
		wg:         &sync.WaitGroup{},
		log:        logger.GetLogger(),
		symbols:    symbols,
		limiter:    limiter,
	}
}

// Okx_FOBS_Start launches snapshot workers for each configured symbol.
func (r *Okx_FOBS_Reader) Okx_FOBS_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.Orderbook.Snapshots
	log := r.log.WithComponent("okx_reader").WithFields(logger.Fields{"operation": "Okx_FOBS_Start"})
	if !cfg.Enabled {
		log.Warn("okx futures orderbook snapshots are disabled")
		return fmt.Errorf("okx futures orderbook snapshots are disabled")
	}
	log.WithFields(logger.Fields{"symbols": r.symbols, "interval": cfg.IntervalMs}).Info("starting okx snapshot reader")
	for _, sym := range r.symbols {
		r.wg.Add(1)
		go r.fetchWorker(sym, cfg)
	}
	log.Info("okx snapshot reader started successfully")
	return nil
}

// Okx_FOBS_Stop stops all snapshot workers.
func (r *Okx_FOBS_Reader) Okx_FOBS_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
	r.log.WithComponent("okx_reader").Info("stopping okx snapshot reader")
	r.wg.Wait()
	r.log.WithComponent("okx_reader").Info("okx snapshot reader stopped")
}

// fetchWorker periodically pulls snapshot for a single symbol.
func (r *Okx_FOBS_Reader) fetchWorker(symbol string, snapshotCfg config.OkxSnapshotConfig) {
	defer r.wg.Done()
	log := r.log.WithComponent("okx_reader").WithFields(logger.Fields{"symbol": symbol, "worker": "orderbook_fetcher"})
	interval := time.Duration(snapshotCfg.IntervalMs) * time.Millisecond
	now := time.Now()
	next := now.Truncate(interval).Add(interval)
	timer := time.NewTimer(next.Sub(now))
	defer timer.Stop()
	for {
		select {
		case <-r.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case <-timer.C:
			start := time.Now()
			r.fetchOrderbook(symbol, snapshotCfg)
			duration := time.Since(start)
			if duration > interval {
				log.WithFields(logger.Fields{"duration": duration.Milliseconds(), "interval": snapshotCfg.IntervalMs}).Warn("fetch took longer than interval")
			}
			next = start.Truncate(interval).Add(interval)
			timer.Reset(time.Until(next))
		}
	}
}

// fetchOrderbook pulls snapshot data for a symbol and forwards to raw channel.
func (r *Okx_FOBS_Reader) fetchOrderbook(symbol string, snapshotCfg config.OkxSnapshotConfig) {
	log := r.log.WithComponent("okx_reader").WithFields(logger.Fields{"symbol": symbol, "operation": "fetch_orderbook"})
	params := map[string]string{
		"instId":   symbol,
		"instType": "SWAP",
		"sz":       strconv.Itoa(snapshotCfg.Limit),
	}
	if r.limiter != nil {
		if err := r.limiter.Wait(r.ctx); err != nil {
			log.WithError(err).Warn("rate limiter wait failed")
			return
		}
	}

	res, err := r.client.Do(http.MethodGet, "/api/v5/market/books", false, params)
	if err != nil {
		if strings.Contains(err.Error(), "50011") || strings.Contains(strings.ToLower(err.Error()), "rate limit") {
			log.WithError(err).Warn("rate limited, backing off")
			time.Sleep(time.Second)
		} else {
			log.WithError(err).Warn("failed to fetch orderbook")
		}
		return
	}
	defer res.Body.Close()
	var obRes marketres.OrderBook
	if err := json.NewDecoder(res.Body).Decode(&obRes); err != nil {
		log.WithError(err).Warn("failed to decode orderbook")
		return
	}
	if len(obRes.OrderBooks) == 0 {
		log.Warn("empty orderbook response")
		return
	}
	book := obRes.OrderBooks[0]
	bids := make([][]string, len(book.Bids))
	for i, b := range book.Bids {
		bids[i] = []string{fmt.Sprintf("%f", b.DepthPrice), fmt.Sprintf("%f", b.Size)}
	}
	asks := make([][]string, len(book.Asks))
	for i, a := range book.Asks {
		asks[i] = []string{fmt.Sprintf("%f", a.DepthPrice), fmt.Sprintf("%f", a.Size)}
	}
	resp := models.BinanceFOBSresp{LastUpdateID: time.Time(book.TS).UnixMilli(), Bids: bids, Asks: asks}
	payload, err := json.Marshal(resp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}
	msg := models.RawFOBSMessage{
		Exchange:    "okx",
		Symbol:      symbol,
		Market:      "future-orderbook-snapshot",
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}
	select {
	case r.rawChannel <- msg:
		logger.LogDataFlowEntry(log, "okx_api", "raw_channel", len(asks)+len(bids), "orderbook_entries")
		logger.IncrementSnapshotRead(len(payload))
	case <-r.ctx.Done():
		return
	default:
		log.Warn("raw channel is full, dropping data")
	}
}

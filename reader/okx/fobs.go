package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cryptoflow/config"
	fobs "cryptoflow/internal/channel/fobs"
	"cryptoflow/logger"
	"cryptoflow/models"

	okex "github.com/tfxq/okx-go-sdk"
	okxapi "github.com/tfxq/okx-go-sdk/api"
	marketmodel "github.com/tfxq/okx-go-sdk/models/market"
	marketreq "github.com/tfxq/okx-go-sdk/requests/rest/market"

	"golang.org/x/time/rate"
)

// Okx_FOBS_Reader periodically queries the OKX REST API for swap order book
// snapshots and forwards the data into the raw snapshot channel.  The reader
// leverages the official okx-go-sdk for request construction and response
// parsing to ensure compatibility and performance.
type Okx_FOBS_Reader struct {
	config   *config.Config
	channels *fobs.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	limiter  *rate.Limiter
	api      *okxapi.Client
	localIP  string
}

// Okx_FOBS_NewReader constructs a new snapshot reader instance.  Network
// resources are not allocated until Start is invoked.
func Okx_FOBS_NewReader(cfg *config.Config, ch *fobs.Channels, symbols []string, localIP string) *Okx_FOBS_Reader {
	rl := cfg.Reader.RateLimit
	rps := rl.RequestsPerSecond
	if rps <= 0 {
		rps = 5
	}
	burst := rl.BurstSize
	if burst <= 0 {
		burst = 1
	}
	return &Okx_FOBS_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		limiter:  rate.NewLimiter(rate.Limit(rps), burst),
		localIP:  localIP,
	}
}

// Okx_FOBS_Start launches snapshot fetch workers for each configured symbol.
// A custom HTTP transport with connection pooling is installed into the
// okx-go-sdk before any requests are executed.
func (r *Okx_FOBS_Reader) Okx_FOBS_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	transport := &http.Transport{
		MaxIdleConns:        r.config.Source.Okx.ConnectionPool.MaxIdleConns,
		MaxIdleConnsPerHost: r.config.Source.Okx.ConnectionPool.MaxIdleConns,
		MaxConnsPerHost:     r.config.Source.Okx.ConnectionPool.MaxConnsPerHost,
		IdleConnTimeout:     r.config.Source.Okx.ConnectionPool.IdleConnTimeout,
	}
	if r.localIP != "" {
		if ip := net.ParseIP(r.localIP); ip != nil {
			dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}
			transport.DialContext = dialer.DialContext
		}
	}
	http.DefaultClient = &http.Client{Transport: transport, Timeout: r.config.Reader.Timeout}

	// Initialise the OKX REST client using the configured HTTP client.
	apiClient, err := okxapi.NewClient(ctx, "", "", "", okex.NormalServer)
	if err != nil {
		r.log.WithComponent("okx_reader").WithError(err).Error("failed to create okx rest client")
		return err
	}
	r.api = apiClient

	cfg := r.config.Source.Okx.Future.Orderbook.Snapshots
	log := r.log.WithComponent("okx_reader").WithFields(logger.Fields{"operation": "Okx_FOBS_Start"})
	if !cfg.Enabled {
		log.Warn("okx swap orderbook snapshots are disabled")
		return fmt.Errorf("okx swap orderbook snapshots are disabled")
	}

	log.WithFields(logger.Fields{"symbols": r.symbols, "interval": cfg.IntervalMs}).Info("starting okx swap snapshot reader")
	for _, sym := range r.symbols {
		r.wg.Add(1)
		go r.fetchWorker(sym, cfg)
	}
	log.Info("okx swap snapshot reader started successfully")
	return nil
}

// Okx_FOBS_Stop halts all running snapshot workers and waits for completion.
func (r *Okx_FOBS_Reader) Okx_FOBS_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
	r.log.WithComponent("okx_reader").Info("stopping okx snapshot reader")
	r.wg.Wait()
	r.log.WithComponent("okx_reader").Info("okx snapshot reader stopped")
}

// fetchWorker pulls snapshots for a single symbol at the configured interval.
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

// fetchOrderbook queries the REST API for a symbol and forwards the data to the
// raw snapshot channel.
func (r *Okx_FOBS_Reader) fetchOrderbook(symbol string, snapshotCfg config.OkxSnapshotConfig) {
	log := r.log.WithComponent("okx_reader").WithFields(logger.Fields{"symbol": symbol, "operation": "fetch_orderbook"})
	if err := r.limiter.Wait(r.ctx); err != nil {
		log.WithError(err).Warn("rate limiter wait failed")
		return
	}

	book, err := r.getMarketBooksFull(symbol, snapshotCfg.Limit)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook from okx")
		return
	}
	ts := time.Time(book.TS).UnixMilli()
	bids := make([][]string, len(book.Bids))
	for i, b := range book.Bids {
		bids[i] = []string{fmt.Sprintf("%f", b.DepthPrice), fmt.Sprintf("%f", b.Size)}
	}
	asks := make([][]string, len(book.Asks))
	for i, a := range book.Asks {
		asks[i] = []string{fmt.Sprintf("%f", a.DepthPrice), fmt.Sprintf("%f", a.Size)}
	}

	snapshot := models.BinanceFOBSresp{LastUpdateID: ts, Bids: bids, Asks: asks}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		log.WithError(err).Warn("failed to marshal snapshot")
		return
	}

	msg := models.RawFOBSMessage{
		Exchange:    "okx",
		Symbol:      symbol,
		Market:      "swap-orderbook-snapshot",
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}
	if r.channels.SendRaw(r.ctx, msg) {
		logger.LogDataFlowEntry(log, "okx_rest", "raw_channel", len(asks)+len(bids), "orderbook_entries")
		logger.IncrementSnapshotRead(len(payload))
	} else if r.ctx.Err() != nil {
		return
	} else {
		log.Warn("raw snapshot channel full, dropping data")
	}
}

// getMarketBooksFull retrieves the full depth order book snapshot for a symbol
// using the OKX REST API. It is a thin wrapper around the SDK's order book
// endpoint but provides clearer intent within the reader implementation.
func (r *Okx_FOBS_Reader) getMarketBooksFull(symbol string, limit int) (*marketmodel.OrderBook, error) {
	req := marketreq.GetOrderBook{InstID: symbol, Sz: limit}
	resp, err := r.api.Rest.Market.GetOrderBook(req)
	if err != nil {
		return nil, err
	}
	if len(resp.OrderBooks) == 0 {
		return nil, fmt.Errorf("empty orderbook response")
	}
	return resp.OrderBooks[0], nil
}

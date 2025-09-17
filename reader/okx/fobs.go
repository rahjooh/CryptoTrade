package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"cryptoflow/config"
	fobs "cryptoflow/internal/channel/fobs"
	okxmetrics "cryptoflow/internal/metrics/okx"
	"cryptoflow/logger"
	"cryptoflow/models"
	"golang.org/x/time/rate"
)

// Okx_FOBS_Reader periodically queries the OKX REST API for swap order book
// snapshots and forwards the data into the raw snapshot channel. The reader
// issues HTTP requests directly without relying on any third-party SDK.
type Okx_FOBS_Reader struct {
	config     *config.Config
	channels   *fobs.Channels
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log
	symbols    []string
	limiter    *rate.Limiter
	httpClient *http.Client
	localIP    string
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
// A custom HTTP transport with connection pooling is configured for the HTTP
// client used for REST calls.
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
	r.httpClient = &http.Client{Transport: userAgentTransport{agent: "curl/8.5.0", base: transport}, Timeout: r.config.Reader.Timeout}
	r.symbols = r.validateSymbols(r.symbols)

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

	book, header, err := r.getMarketBooksFull(symbol, snapshotCfg.Limit)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook from okx")
		return
	}
	if header != nil {
		rl := okxmetrics.ExtractRateLimit(header)
		deltaCfg := r.config.Source.Okx.Future.Orderbook.Delta
		symbolCount := len(deltaCfg.Symbols)
		if symbolCount == 0 {
			symbolCount = len(r.symbols)
		}
		var estimatedExtra float64
		if symbolCount > 0 {
			totalEstimate := okxmetrics.EstimateWebsocketConnectionPressure(1)
			if totalEstimate > 0 {
				estimatedExtra = totalEstimate / float64(symbolCount)
			}
		}
		okxmetrics.ReportUsage(r.log, "okx_reader", symbol, "swap-orderbook-snapshot", r.localIP, rl, okxmetrics.SnapshotWeightPerRequest, estimatedExtra)
		log.WithFields(logger.Fields{
			"rate_limit":     header.Get("Rate-Limit-Limit"),
			"rate_remaining": header.Get("Rate-Limit-Remaining"),
			"rate_reset":     header.Get("Rate-Limit-Reset"),
		}).Debug("okx rate limit status")
	}
	if len(book.Bids) == 0 && len(book.Asks) == 0 {
		log.Warn("received empty orderbook snapshot")
		return
	}
	ts, _ := strconv.ParseInt(book.Ts, 10, 64)
	bids := make([][]string, len(book.Bids))
	for i, b := range book.Bids {
		//bids[i] = []string{fmt.Sprintf("%f", b.DepthPrice), fmt.Sprintf("%f", b.Size)}
		if len(b) >= 2 {
			bids[i] = []string{b[0], b[1]}
		}
	}
	asks := make([][]string, len(book.Asks))
	for i, a := range book.Asks {
		//asks[i] = []string{fmt.Sprintf("%f", a.DepthPrice), fmt.Sprintf("%f", a.Size)}
		if len(a) >= 2 {
			asks[i] = []string{a[0], a[1]}
		}
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
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
			"entries":       len(asks) + len(bids),
		}).Info("orderbook data sent to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		log.Warn("raw snapshot channel full, dropping data")
	}
}

type okxOrderBook struct {
	Bids [][]string `json:"bids"`
	Asks [][]string `json:"asks"`
	Ts   string     `json:"ts"`
}

// getMarketBooksFull retrieves the full depth order book snapshot for a symbol
// by calling the OKX REST API directly.
func (r *Okx_FOBS_Reader) getMarketBooksFull(symbol string, limit int) (*okxOrderBook, http.Header, error) {
	url := fmt.Sprintf("https://www.okx.com/api/v5/market/books-full?instId=%s&sz=%d", symbol, limit)
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	var wrapper struct {
		Code string         `json:"code"`
		Data []okxOrderBook `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		return nil, resp.Header, err
	}
	if len(wrapper.Data) == 0 {
		return nil, resp.Header, fmt.Errorf("empty orderbook response")
	}
	return &wrapper.Data[0], resp.Header, nil
}

func (r *Okx_FOBS_Reader) validateSymbols(symbols []string) []string {
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", nil)
	if err != nil {
		r.log.WithComponent("okx_reader").WithError(err).Warn("failed to build instruments request")
		return symbols
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		r.log.WithComponent("okx_reader").WithError(err).Warn("failed to fetch instruments list")
		return symbols
	}
	defer resp.Body.Close()
	var wrapper struct {
		Code string `json:"code"`
		Data []struct {
			InstID string `json:"instId"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		r.log.WithComponent("okx_reader").WithError(err).Warn("failed to decode instruments list")
		return symbols
	}
	valid := make(map[string]struct{}, len(wrapper.Data))
	for _, inst := range wrapper.Data {
		valid[inst.InstID] = struct{}{}
	}
	var filtered []string
	for _, s := range symbols {
		if _, ok := valid[s]; ok {
			filtered = append(filtered, s)
		} else {
			r.log.WithComponent("okx_reader").WithFields(logger.Fields{"symbol": s}).Warn("invalid instrument, skipping")
		}
	}
	return filtered
}

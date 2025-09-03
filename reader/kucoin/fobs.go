package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// Kucoin_FOBS_Reader fetches futures order book snapshots from KuCoin.
type Kucoin_FOBS_Reader struct {
	config      *config.Config
	client      *http.Client
	rawChannel  chan<- models.RawFOBSMessage
	ctx         context.Context
	wg          *sync.WaitGroup
	mu          sync.RWMutex
	running     bool
	log         *logger.Log
	symbols     []string
	snapshotURL string
}

// Kucoin_FOBS_NewReader creates a new Kucoin_FOBS_Reader using a custom HTTP client.
// The reader will bind outbound connections to the provided localIP if not empty
// and fetch snapshots only for the supplied symbols.
func Kucoin_FOBS_NewReader(cfg *config.Config, rawChannel chan<- models.RawFOBSMessage, symbols []string, localIP string) *Kucoin_FOBS_Reader {
	log := logger.GetLogger()

	snapshotCfg := cfg.Source.Kucoin.Future.Orderbook.Snapshots

	transport := &http.Transport{
		MaxIdleConns:        cfg.Source.Kucoin.ConnectionPool.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.Source.Kucoin.ConnectionPool.MaxIdleConns,
		MaxConnsPerHost:     cfg.Source.Kucoin.ConnectionPool.MaxConnsPerHost,
		IdleConnTimeout:     cfg.Source.Kucoin.ConnectionPool.IdleConnTimeout,
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

	reader := &Kucoin_FOBS_Reader{
		config:      cfg,
		client:      httpClient,
		rawChannel:  rawChannel,
		wg:          &sync.WaitGroup{},
		log:         log,
		symbols:     symbols,
		snapshotURL: snapshotCfg.URL,
	}

	log.WithComponent("kucoin_reader").WithFields(logger.Fields{
		"base_url": snapshotCfg.URL,
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

	reqURL, err := url.Parse(r.snapshotURL)
	if err != nil {
		log.WithError(err).Warn("invalid snapshot URL")
		return
	}
	q := reqURL.Query()
	q.Set("symbol", symbol)
	reqURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		log.WithError(err).Warn("failed to create request")
		return
	}

	res, err := r.client.Do(req)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook")
		return
	}
	defer res.Body.Close()

	var resp struct {
		Code string `json:"code"`
		Data struct {
			Sequence int64       `json:"sequence"`
			Bids     [][]float64 `json:"bids"`
			Asks     [][]float64 `json:"asks"`
		} `json:"data"`
	}

	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		log.WithError(err).Warn("failed to decode snapshot data")
		return
	}

	bids := make([][]string, len(resp.Data.Bids))
	for i, b := range resp.Data.Bids {
		if len(b) < 2 {
			continue
		}
		bids[i] = []string{
			strconv.FormatFloat(b[0], 'f', -1, 64),
			strconv.FormatFloat(b[1], 'f', -1, 64),
		}
	}

	asks := make([][]string, len(resp.Data.Asks))
	for i, a := range resp.Data.Asks {
		if len(a) < 2 {
			continue
		}
		asks[i] = []string{
			strconv.FormatFloat(a[0], 'f', -1, 64),
			strconv.FormatFloat(a[1], 'f', -1, 64),
		}
	}

	kucoinResp := models.BinanceFOBSresp{
		LastUpdateID: resp.Data.Sequence,
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

package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"cryptoflow/config"
	fobs "cryptoflow/internal/channel/fobs"
	ratemetrics "cryptoflow/internal/metrics/rate"
	"cryptoflow/logger"
	"cryptoflow/models"

	futures "github.com/adshao/go-binance/v2/futures"
)

// Binance_FOBS_Reader fetches futures order book snapshots from Binance.
type Binance_FOBS_Reader struct {
	config      *config.Config
	client      *futures.Client
	channels    *fobs.Channels
	ctx         context.Context
	wg          *sync.WaitGroup
	mu          sync.RWMutex
	running     bool
	log         *logger.Log
	symbols     []string
	weightLimit int64
}

// Binance_FOBS_NewReader creates a new Binance_FOBS_Reader using the binance-go client.
// The reader will bind outbound connections to the provided localIP if not empty
// and fetch snapshots only for the supplied symbols.
func Binance_FOBS_NewReader(cfg *config.Config, ch *fobs.Channels, symbols []string, localIP string) *Binance_FOBS_Reader {
	log := logger.GetLogger()

	transport := &http.Transport{
		MaxIdleConns:        cfg.Source.Binance.ConnectionPool.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.Source.Binance.ConnectionPool.MaxIdleConns,
		MaxConnsPerHost:     cfg.Source.Binance.ConnectionPool.MaxConnsPerHost,
		IdleConnTimeout:     cfg.Source.Binance.ConnectionPool.IdleConnTimeout,
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

	client := futures.NewClient("", "")
	client.HTTPClient = httpClient

	snapshotCfg := cfg.Source.Binance.Future.Orderbook.Snapshots
	if parsed, err := url.Parse(snapshotCfg.URL); err == nil {
		base := fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
		client.SetApiEndpoint(base)
	}

	reader := &Binance_FOBS_Reader{
		config:   cfg,
		client:   client,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      log,
		symbols:  symbols,
	}

	log.WithComponent("binance_reader").WithFields(logger.Fields{
		"max_idle_conns":     cfg.Source.Binance.ConnectionPool.MaxIdleConns,
		"max_conns_per_host": cfg.Source.Binance.ConnectionPool.MaxConnsPerHost,
		"timeout":            cfg.Reader.Timeout,
	}).Info("binance reader initialized")

	return reader
}

// Binance_FOBS_Start begins fetching order book snapshots for configured symbols.
func (br *Binance_FOBS_Reader) Binance_FOBS_Start(ctx context.Context) error {
	br.mu.Lock()
	if br.running {
		br.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	br.running = true
	br.ctx = ctx
	br.mu.Unlock()

	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{"operation": "Binance_FOBS_Start"})

	snapshotCfg := br.config.Source.Binance.Future.Orderbook.Snapshots
	if !snapshotCfg.Enabled {
		log.Warn("binance futures orderbook snapshots are disabled")
		return fmt.Errorf("binance futures orderbook snapshots are disabled")
	}

	if limit, err := ratemetrics.FetchRequestWeightLimit(ctx, br.client); err == nil {
		br.weightLimit = limit
	} else {
		log.WithError(err).Warn("failed to fetch request weight limit")
	}

	log.WithFields(logger.Fields{
		"symbols":  br.symbols,
		"interval": snapshotCfg.IntervalMs,
	}).Info("starting binance reader")

	for _, symbol := range br.symbols {
		br.wg.Add(1)
		go br.fetchOrderbookWorker(symbol, snapshotCfg)
	}

	log.Info("binance reader started successfully")
	return nil
}

// Binance_FOBS_Stop signals all workers to stop and waits for completion.
func (br *Binance_FOBS_Reader) Binance_FOBS_Stop() {
	br.mu.Lock()
	br.running = false
	br.mu.Unlock()

	br.log.WithComponent("binance_reader").Info("stopping binance reader")
	br.wg.Wait()
	br.log.WithComponent("binance_reader").Info("binance reader stopped")
}

func (br *Binance_FOBS_Reader) fetchOrderbookWorker(symbol string, snapshotCfg config.BinanceSnapshotConfig) {
	defer br.wg.Done()

	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{
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
		case <-br.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case <-timer.C:
			start := time.Now()
			br.fetchOrderbook(symbol, snapshotCfg)
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

func (br *Binance_FOBS_Reader) fetchOrderbook(symbol string, snapshotCfg config.BinanceSnapshotConfig) {
	log := br.log.WithComponent("binance_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_orderbook",
	})

	market := "future-orderbook-snapshot"

	start := time.Now()
	reqURL := fmt.Sprintf("%s?symbol=%s&limit=%d", snapshotCfg.URL, symbol, snapshotCfg.Limit)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		log.WithError(err).Warn("failed to build request")
		return
	}
	req = req.WithContext(br.ctx)
	resp, err := br.client.HTTPClient.Do(req)

	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook")
		return
	}
	duration := time.Since(start)
	defer resp.Body.Close()
	logger.LogPerformanceEntry(log, "binance_reader", "api_request", duration, logger.Fields{
		"symbol": symbol,
	})

	ratemetrics.ReportSnapshotWeight(br.log, resp.Header, br.weightLimit, snapshotCfg.Limit)

	var binanceResp models.BinanceFOBSresp
	if err := json.NewDecoder(resp.Body).Decode(&binanceResp); err != nil {
		log.WithError(err).Warn("failed to decode orderbook")
		return
	}

	payload, err := json.Marshal(binanceResp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}

	rawData := models.RawFOBSMessage{
		Exchange:    "binance",
		Symbol:      symbol,
		Market:      market,
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}

	if br.channels.SendRaw(br.ctx, rawData) {
		log.Info("orderbook data sent to raw channel")
		logger.LogDataFlowEntry(log, "binance_api", "raw_channel", len(binanceResp.Bids)+len(binanceResp.Asks), "orderbook_entries")
		logger.IncrementSnapshotRead(len(payload))
	} else if br.ctx.Err() != nil {
		return
	} else {
		log.Warn("raw channel is full, dropping data")
	}
}

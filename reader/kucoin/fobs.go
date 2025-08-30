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

	kumex "github.com/Kucoin/kucoin-futures-go-sdk"
)

// KucoinReader fetches futures order book snapshots from KuCoin.
type KucoinReader struct {
	config     *config.Config
	client     *kumex.ApiService
	rawChannel chan<- models.RawOrderbookMessage
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log
}

// NewKucoinReader creates a new KucoinReader using the KuCoin futures SDK.
func NewKucoinReader(cfg *config.Config, rawChannel chan<- models.RawOrderbookMessage) *KucoinReader {
	log := logger.GetLogger()

	snapshotCfg := cfg.Source.Kucoin.Future.Orderbook.Snapshots

	baseURL := snapshotCfg.URL
	if parsed, err := url.Parse(snapshotCfg.URL); err == nil {
		baseURL = fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
	}

	client := kumex.NewApiService(
		kumex.ApiBaseURIOption(baseURL),
	)

	reader := &KucoinReader{
		config:     cfg,
		client:     client,
		rawChannel: rawChannel,
		wg:         &sync.WaitGroup{},
		log:        log,
	}

	log.WithComponent("kucoin_reader").WithFields(logger.Fields{
		"base_url": baseURL,
	}).Info("kucoin reader initialized")

	return reader
}

// Start begins fetching order book snapshots for configured symbols.
func (r *KucoinReader) start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	log := r.log.WithComponent("kucoin_reader").WithFields(logger.Fields{"operation": "start"})

	snapshotCfg := r.config.Source.Kucoin.Future.Orderbook.Snapshots
	if !snapshotCfg.Enabled {
		log.Warn("kucoin futures orderbook snapshots are disabled")
		return fmt.Errorf("kucoin futures orderbook snapshots are disabled")
	}

	log.WithFields(logger.Fields{
		"symbols":  snapshotCfg.Symbols,
		"interval": snapshotCfg.IntervalMs,
	}).Info("starting kucoin reader")

	for _, symbol := range snapshotCfg.Symbols {
		r.wg.Add(1)
		go r.fetchOrderbookWorker(symbol, snapshotCfg)
	}

	log.Info("kucoin reader started successfully")
	return nil
}

// Stop signals all workers to stop and waits for completion.
func (r *KucoinReader) stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_reader").Info("stopping kucoin reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_reader").Info("kucoin reader stopped")
}

func (r *KucoinReader) fetchOrderbookWorker(symbol string, snapshotCfg config.KucoinSnapshotConfig) {
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
			r.fetchOrderbook(symbol)
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

func (r *KucoinReader) fetchOrderbook(symbol string) {
	log := r.log.WithComponent("kucoin_reader").WithFields(logger.Fields{
		"symbol":    symbol,
		"operation": "fetch_orderbook",
	})

	market := "future-orderbook-snapshot"

	res, err := r.client.Level2Snapshot(symbol)
	if err != nil {
		log.WithError(err).Warn("failed to fetch orderbook")
		return
	}

	snap := kumex.Level2SnapshotModel{}
	if err := res.ReadData(&snap); err != nil {
		log.WithError(err).Warn("failed to read snapshot data")
		return
	}

	bids := make([][]string, len(snap.Bids))
	for i, b := range snap.Bids {
		bids[i] = []string{
			strconv.FormatFloat(float64(b[0]), 'f', -1, 32),
			strconv.FormatFloat(float64(b[1]), 'f', -1, 32),
		}
	}
	asks := make([][]string, len(snap.Asks))
	for i, a := range snap.Asks {
		asks[i] = []string{
			strconv.FormatFloat(float64(a[0]), 'f', -1, 32),
			strconv.FormatFloat(float64(a[1]), 'f', -1, 32),
		}
	}

	kucoinResp := models.BinanceFOBSresponceModel{
		LastUpdateID: int64(snap.Sequence),
		Bids:         bids,
		Asks:         asks,
	}

	payload, err := json.Marshal(kucoinResp)
	if err != nil {
		log.WithError(err).Warn("failed to marshal orderbook")
		return
	}

	rawData := models.RawOrderbookMessage{
		Exchange:    "kucoin",
		Symbol:      symbols.NormalizeKucoinSymbol(symbol),
		Market:      market,
		Timestamp:   time.Now().UTC(),
		Data:        payload,
		MessageType: "snapshot",
	}

	select {
	case r.rawChannel <- rawData:
		log.Info("orderbook data sent to raw channel")
		logger.LogDataFlowEntry(log, "kucoin_api", "raw_channel", len(snap.Bids)+len(snap.Asks), "orderbook_entries")
		logger.IncrementSnapshotRead(len(payload))
	case <-r.ctx.Done():
		return
	default:
		log.Warn("raw channel is full, dropping data")
	}
}

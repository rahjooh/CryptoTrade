package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// DeltaProcessor normalizes order book delta messages and batches them
// before forwarding to the next stage.
type DeltaProcessor struct {
	config   *appconfig.Config
	rawChan  <-chan models.RawFOBDMessage
	normChan chan<- models.BatchFOBDMessage
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log

	batches   map[string]*models.BatchFOBDMessage
	lastFlush map[string]time.Time

	symbols       map[string]struct{}
	filterSymbols bool
}

// NewDeltaProcessor creates a new processor instance.
func NewDeltaProcessor(cfg *appconfig.Config, rawChan <-chan models.RawFOBDMessage, normChan chan<- models.BatchFOBDMessage) *DeltaProcessor {
	symSet := make(map[string]struct{})
	for _, s := range cfg.Source.Binance.Future.Orderbook.Delta.Symbols {
		symSet[s] = struct{}{}
	}
	for _, s := range cfg.Source.Kucoin.Future.Orderbook.Delta.Symbols {
		symSet[symbols.ToBinance("kucoin", s)] = struct{}{}
	}
	return &DeltaProcessor{
		config:        cfg,
		rawChan:       rawChan,
		normChan:      normChan,
		wg:            &sync.WaitGroup{},
		log:           logger.GetLogger(),
		batches:       make(map[string]*models.BatchFOBDMessage),
		lastFlush:     make(map[string]time.Time),
		symbols:       symSet,
		filterSymbols: len(symSet) > 0,
	}
}

// start begins processing messages from the raw delta channel.
func (p *DeltaProcessor) start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("delta processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	log := p.log.WithComponent("delta_processor").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting delta processor")

	workers := p.config.Processor.MaxWorkers
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	p.wg.Add(1)
	go p.flusher()

	p.wg.Add(1)
	go p.metricsReporter(ctx)

	log.Info("delta processor started successfully")
	return nil
}

// Stop signals all workers and flushes remaining batches.
func (p *DeltaProcessor) stop() {
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("delta_processor").Info("stopping delta processor")
	p.flushAll()
	p.wg.Wait()
	p.log.WithComponent("delta_processor").Info("delta processor stopped")
}

// Start exposes the start method for external callers.
func (p *DeltaProcessor) Start(ctx context.Context) error { return p.start(ctx) }

// Stop exposes the stop method for external callers.
func (p *DeltaProcessor) Stop() { p.stop() }

func (p *DeltaProcessor) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-p.rawChan:
			if !ok {
				return
			}
			if p.filterSymbols {
				if _, ok := p.symbols[msg.Symbol]; !ok {
					// drop unneeded symbol
					continue
				}
			}
			p.handleMessage(msg)
		}
	}
}

func (p *DeltaProcessor) handleMessage(raw models.RawFOBDMessage) {
	log := p.log.WithComponent("delta_processor").WithFields(logger.Fields{
		"symbol":   raw.Symbol,
		"exchange": raw.Exchange,
	})

	var (
		eventTime     int64
		updateID      int64
		prevUpdateID  int64
		firstUpdateID int64
		bids          []models.FOBDEntry
		asks          []models.FOBDEntry
	)

	switch raw.Exchange {
	case "kucoin":
		var evt models.KucoinFOBDResp
		if err := json.Unmarshal(raw.Data, &evt); err != nil {
			log.WithError(err).Warn("failed to unmarshal delta message")
			return
		}
		eventTime = evt.Timestamp
		updateID = evt.Sequence
		prevUpdateID = evt.Sequence - 1
		firstUpdateID = evt.Sequence
		bids = evt.Bids
		asks = evt.Asks
	default:
		var evt models.BinanceFOBDResp
		if err := json.Unmarshal(raw.Data, &evt); err != nil {
			log.WithError(err).Warn("failed to unmarshal delta message")
			return
		}
		eventTime = evt.Time
		updateID = evt.LastUpdateID
		prevUpdateID = evt.PrevLastUpdateID
		firstUpdateID = evt.FirstUpdateID
		bids = evt.Bids
		asks = evt.Asks
	}

	entries := make([]models.NormFOBDMessage, 0, len(bids)+len(asks))
	recv := raw.Timestamp.UnixMilli()
	for _, b := range bids {
		price, err1 := strconv.ParseFloat(b.Price, 64)
		qty, err2 := strconv.ParseFloat(b.Quantity, 64)
		if err1 != nil || err2 != nil || price == 0 || qty == 0 {
			continue
		}
		entries = append(entries, models.NormFOBDMessage{
			Symbol:        raw.Symbol,
			EventTime:     eventTime,
			UpdateID:      updateID,
			PrevUpdateID:  prevUpdateID,
			FirstUpdateID: firstUpdateID,
			Side:          "bid",
			Price:         price,
			Quantity:      qty,
			ReceivedTime:  recv,
		})
	}
	for _, a := range asks {
		price, err1 := strconv.ParseFloat(a.Price, 64)
		qty, err2 := strconv.ParseFloat(a.Quantity, 64)
		if err1 != nil || err2 != nil || price == 0 || qty == 0 {
			continue
		}
		entries = append(entries, models.NormFOBDMessage{
			Symbol:        raw.Symbol,
			EventTime:     eventTime,
			UpdateID:      updateID,
			PrevUpdateID:  prevUpdateID,
			FirstUpdateID: firstUpdateID,
			Side:          "ask",
			Price:         price,
			Quantity:      qty,
			ReceivedTime:  recv,
		})
	}

	if len(entries) == 0 {
		return
	}

	p.addToBatch(raw, entries)
}

func (p *DeltaProcessor) addToBatch(raw models.RawFOBDMessage, entries []models.NormFOBDMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s_%s_%s", raw.Exchange, raw.Market, raw.Symbol)
	batch, ok := p.batches[key]
	if !ok {
		batch = &models.BatchFOBDMessage{
			BatchID:     uuid.New().String(),
			Exchange:    raw.Exchange,
			Symbol:      raw.Symbol,
			Market:      raw.Market,
			Entries:     make([]models.NormFOBDMessage, 0, p.config.Processor.BatchSize),
			Timestamp:   raw.Timestamp,
			ProcessedAt: time.Now(),
		}
		p.batches[key] = batch
		p.lastFlush[key] = time.Now()
	}

	batch.Entries = append(batch.Entries, entries...)
	batch.RecordCount = len(batch.Entries)
	if raw.Timestamp.After(batch.Timestamp) {
		batch.Timestamp = raw.Timestamp
	}

	if batch.RecordCount >= p.config.Processor.BatchSize {
		p.flush(key)
	}
}

func (p *DeltaProcessor) flusher() {
	defer p.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.flushTimedOut()
		}
	}
}

func (p *DeltaProcessor) flushTimedOut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, t := range p.lastFlush {
		if now.Sub(t) >= p.config.Processor.BatchTimeout {
			p.flush(k)
		}
	}
}

func (p *DeltaProcessor) flush(key string) {
	batch, ok := p.batches[key]
	if !ok || batch.RecordCount == 0 {
		return
	}
	select {
	case p.normChan <- *batch:
		delete(p.batches, key)
		delete(p.lastFlush, key)
	case <-p.ctx.Done():
		return
	default:
		p.log.WithComponent("delta_processor").WithFields(logger.Fields{"batch_key": key}).Warn("normfobd channel full, dropping batch")
	}
}

func (p *DeltaProcessor) flushAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k := range p.batches {
		p.flush(k)
	}
}

func (p *DeltaProcessor) metricsReporter(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			running := p.running
			p.mu.RUnlock()
			if !running {
				return
			}
			p.log.WithComponent("delta_processor").WithFields(logger.Fields{
				"raw_channel_len":  len(p.rawChan),
				"raw_channel_cap":  cap(p.rawChan),
				"norm_channel_len": len(p.normChan),
				"norm_channel_cap": cap(p.normChan),
			}).Info("delta processor channel sizes")
		}
	}
}

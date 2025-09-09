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
	fobd "cryptoflow/internal/channel/fobd"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// DeltaProcessor normalizes order book delta messages and batches them
// before forwarding to the next stage.
type batchState struct {
	mu        sync.Mutex
	batch     *models.BatchFOBDMessage
	lastFlush time.Time
}

// DeltaProcessor normalizes order book delta messages and batches them
// before forwarding to the next stage.
type DeltaProcessor struct {
	config   *appconfig.Config
	channels *fobd.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log

	batches map[string]*batchState

	symbols       map[string]struct{}
	filterSymbols bool
}

// NewDeltaProcessor creates a new processor instance.
func NewDeltaProcessor(cfg *appconfig.Config, ch *fobd.Channels) *DeltaProcessor {
	symSet := make(map[string]struct{})
	for _, s := range cfg.Source.Binance.Future.Orderbook.Delta.Symbols {
		symSet[s] = struct{}{}
	}
	for _, s := range cfg.Source.Kucoin.Future.Orderbook.Delta.Symbols {
		symSet[symbols.ToBinance("kucoin", s)] = struct{}{}
	}
	for _, s := range cfg.Source.Okx.Future.Orderbook.Delta.Symbols {
		symSet[symbols.ToBinance("okx", s)] = struct{}{}
	}
	for _, s := range cfg.Source.Bybit.Future.Orderbook.Delta.Symbols {
		symSet[symbols.ToBinance("bybit", s)] = struct{}{}
	}
	return &DeltaProcessor{
		config:        cfg,
		channels:      ch,
		wg:            &sync.WaitGroup{},
		log:           logger.GetLogger(),
		batches:       make(map[string]*batchState),
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
		case msg, ok := <-p.channels.Raw:
			if !ok {
				return
			}
			if p.filterSymbols {
				normalized := symbols.ToBinance(msg.Exchange, msg.Symbol)
				if _, ok := p.symbols[normalized]; !ok {
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
	case "bybit":
		var evt models.BybitFOBDResp
		if err := json.Unmarshal(raw.Data, &evt); err != nil {
			log.WithError(err).Warn("failed to unmarshal delta message")
			return
		}
		eventTime = evt.Ts
		updateID = evt.Data.Seq
		prevUpdateID = evt.Data.Seq - 1
		firstUpdateID = evt.Data.Seq
		bids = make([]models.FOBDEntry, 0, len(evt.Data.Bids))
		for _, b := range evt.Data.Bids {
			if len(b) < 2 {
				continue
			}
			bids = append(bids, models.FOBDEntry{Price: b[0], Quantity: b[1]})
		}
		asks = make([]models.FOBDEntry, 0, len(evt.Data.Asks))
		for _, a := range evt.Data.Asks {
			if len(a) < 2 {
				continue
			}
			asks = append(asks, models.FOBDEntry{Price: a[0], Quantity: a[1]})
		}
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
	case "okx":
		var evt models.OkxFOBDResp
		if err := json.Unmarshal(raw.Data, &evt); err != nil {
			log.WithError(err).Warn("failed to unmarshal delta message")
			return
		}
		eventTime = evt.Timestamp
		updateID = evt.Timestamp
		prevUpdateID = evt.Timestamp
		firstUpdateID = evt.Timestamp
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
	normalSymbol := symbols.ToBinance(raw.Exchange, raw.Symbol)
	for _, b := range bids {
		price, err1 := strconv.ParseFloat(b.Price, 64)
		qty, err2 := strconv.ParseFloat(b.Quantity, 64)
		if err1 != nil || err2 != nil || price == 0 || qty == 0 {
			continue
		}
		entries = append(entries, models.NormFOBDMessage{
			Symbol:        normalSymbol,
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
			Symbol:        normalSymbol,
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
	normalSymbol := symbols.ToBinance(raw.Exchange, raw.Symbol)
	key := fmt.Sprintf("%s_%s_%s", raw.Exchange, raw.Market, normalSymbol)

	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		if state, ok = p.batches[key]; !ok {
			state = &batchState{
				batch: &models.BatchFOBDMessage{
					BatchID:     uuid.New().String(),
					Exchange:    raw.Exchange,
					Symbol:      normalSymbol,
					Market:      raw.Market,
					Entries:     make([]models.NormFOBDMessage, 0, p.config.Processor.BatchSize),
					Timestamp:   raw.Timestamp,
					ProcessedAt: time.Now(),
				},
				lastFlush: time.Now(),
			}
			p.batches[key] = state
		}
		p.mu.Unlock()
	}

	state.mu.Lock()
	b := state.batch
	b.Entries = append(b.Entries, entries...)
	b.RecordCount = len(b.Entries)
	if raw.Timestamp.After(b.Timestamp) {
		b.Timestamp = raw.Timestamp
	}
	state.lastFlush = time.Now()
	shouldFlush := b.RecordCount >= p.config.Processor.BatchSize
	state.mu.Unlock()

	if shouldFlush {
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
	p.mu.RLock()
	now := time.Now()
	for k, state := range p.batches {
		state.mu.Lock()
		if now.Sub(state.lastFlush) >= p.config.Processor.BatchTimeout && state.batch.RecordCount > 0 {
			state.mu.Unlock()
			p.flush(k)
		} else {
			state.mu.Unlock()
		}
	}
	p.mu.RUnlock()
}

func (p *DeltaProcessor) flush(key string) {
	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		return
	}

	state.mu.Lock()
	batch := state.batch
	if batch == nil || batch.RecordCount == 0 {
		state.mu.Unlock()
		return
	}
	if p.channels.SendNorm(p.ctx, *batch) {
		state.batch = &models.BatchFOBDMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormFOBDMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	} else if p.ctx.Err() != nil {
		state.mu.Unlock()
		return
	} else {
		p.log.WithComponent("delta_processor").WithFields(logger.Fields{"batch_key": key}).Warn("normfobd channel full, dropping batch")
		state.batch = &models.BatchFOBDMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormFOBDMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	}
	state.mu.Unlock()
}

func (p *DeltaProcessor) flushAll() {
	p.mu.RLock()
	keys := make([]string, 0, len(p.batches))
	for k := range p.batches {
		keys = append(keys, k)
	}
	p.mu.RUnlock()
	for _, k := range keys {
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
				"raw_channel_len":  len(p.channels.Raw),
				"raw_channel_cap":  cap(p.channels.Raw),
				"norm_channel_len": len(p.channels.Norm),
				"norm_channel_cap": cap(p.channels.Norm),
			}).Info("delta processor channel sizes")
		}
	}
}

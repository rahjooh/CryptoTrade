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
	foichannel "cryptoflow/internal/channel/foi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
)

// foiBatchState tracks buffered entries per exchange/symbol bucket.
type foiBatchState struct {
	mu        sync.Mutex
	batch     *models.BatchFOIMessage
	lastFlush time.Time
}

// FOIProcessor normalizes raw open-interest payloads into batches for downstream writers.
type FOIProcessor struct {
	config        *appconfig.Config
	channels      *foichannel.Channels
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
	batches       map[string]*foiBatchState
	symbols       map[string]struct{}
	filterSymbols bool
}

// NewFOIProcessor wires a processor to the provided FOI channels and configuration.
func NewFOIProcessor(cfg *appconfig.Config, ch *foichannel.Channels) *FOIProcessor {
	symSet := make(map[string]struct{})
	if cfg.Source != nil {
		if s := cfg.Source.Binance; s != nil && s.Future != nil && s.Future.OpenInterest.Enabled {
			for _, x := range s.Future.OpenInterest.Symbols {
				symSet[x] = struct{}{}
			}
		}
		if s := cfg.Source.Bybit; s != nil && s.Future != nil && s.Future.OpenInterest.Enabled {
			for _, x := range s.Future.OpenInterest.Symbols {
				symSet[symbols.ToBinance("bybit", x)] = struct{}{}
			}
		}
		if s := cfg.Source.Kucoin; s != nil && s.Future != nil && s.Future.OpenInterest.Enabled {
			for _, x := range s.Future.OpenInterest.Symbols {
				symSet[symbols.ToBinance("kucoin", x)] = struct{}{}
			}
		}
		if s := cfg.Source.Okx; s != nil && s.Future != nil && s.Future.OpenInterest.Enabled {
			for _, x := range s.Future.OpenInterest.Symbols {
				symSet[symbols.ToBinance("okx", x)] = struct{}{}
			}
		}
	}

	filter := len(symSet) > 0
	if !filter {
		// When no source is enabled we still keep map non-nil for future additions.
		symSet = make(map[string]struct{})
	}

	return &FOIProcessor{
		config:        cfg,
		channels:      ch,
		wg:            &sync.WaitGroup{},
		log:           logger.GetLogger(),
		batches:       make(map[string]*foiBatchState),
		symbols:       symSet,
		filterSymbols: filter,
	}
}

// Start begins consuming the raw FOI channel.
func (p *FOIProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("FOI processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	log := p.log.WithComponent("foi_processor").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting FOI processor")

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

	log.Info("FOI processor started successfully")
	return nil
}

// Stop drains buffers and terminates worker goroutines.
func (p *FOIProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("foi_processor").Info("stopping FOI processor")
	p.flushAll()
	p.wg.Wait()
	p.log.WithComponent("foi_processor").Info("FOI processor stopped")
}

func (p *FOIProcessor) worker(id int) {
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

func (p *FOIProcessor) handleMessage(raw models.RawFOIMessage) {
	log := p.log.WithComponent("foi_processor").WithFields(logger.Fields{
		"symbol":   raw.Symbol,
		"exchange": raw.Exchange,
	})

	switch raw.Exchange {
	case "binance":
		p.handleBinance(raw, log)
	case "bybit":
		p.handleBybit(raw, log)
	case "kucoin":
		p.handleKucoin(raw, log)
	case "okx":
		p.handleOkx(raw, log)
	default:
		log.Debug("unsupported FOI exchange, dropping message")
	}
}

func (p *FOIProcessor) handleBinance(raw models.RawFOIMessage, log *logger.Entry) {
	var evt models.BinanceFOICurrentResp
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		log.WithError(err).Warn("failed to unmarshal binance FOI message")
		return
	}

	oi, err := strconv.ParseFloat(evt.OpenInterest, 64)
	if err != nil {
		log.WithError(err).Warn("invalid binance openInterest")
		return
	}

	eventTime := evt.Time
	if eventTime == 0 {
		eventTime = raw.Timestamp.UnixMilli()
	}

	entry := models.NormFOIMessage{
		Symbol:       symbols.ToBinance(raw.Exchange, raw.Symbol),
		EventTime:    eventTime,
		OpenInterest: oi,
		ReceivedTime: raw.Timestamp.UnixMilli(),
	}
	p.addToBatch(raw, entry)
}

func (p *FOIProcessor) handleBybit(raw models.RawFOIMessage, log *logger.Entry) {
	var evt models.BybitFOIResp
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		log.WithError(err).Warn("failed to unmarshal bybit FOI message")
		return
	}
	oi, err := strconv.ParseFloat(evt.OpenInterest, 64)
	if err != nil {
		log.WithError(err).Warn("invalid bybit openInterest")
		return
	}
	entry := models.NormFOIMessage{
		Symbol:       symbols.ToBinance(raw.Exchange, raw.Symbol),
		EventTime:    evt.Ts,
		OpenInterest: oi,
		ReceivedTime: raw.Timestamp.UnixMilli(),
	}
	if entry.EventTime == 0 {
		entry.EventTime = raw.Timestamp.UnixMilli()
	}
	p.addToBatch(raw, entry)
}

func (p *FOIProcessor) handleKucoin(raw models.RawFOIMessage, log *logger.Entry) {
	var evt models.BinanceFOICurrentResp
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		log.WithError(err).Warn("failed to unmarshal kucoin FOI message")
		return
	}
	oi, err := strconv.ParseFloat(evt.OpenInterest, 64)
	if err != nil {
		log.WithError(err).Warn("invalid kucoin openInterest")
		return
	}
	entry := models.NormFOIMessage{
		Symbol:       symbols.ToBinance(raw.Exchange, raw.Symbol),
		EventTime:    evt.Time,
		OpenInterest: oi,
		ReceivedTime: raw.Timestamp.UnixMilli(),
	}
	if entry.EventTime == 0 {
		entry.EventTime = raw.Timestamp.UnixMilli()
	}
	p.addToBatch(raw, entry)
}

func (p *FOIProcessor) handleOkx(raw models.RawFOIMessage, log *logger.Entry) {
	var evt models.OKXFOIResp
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		log.WithError(err).Warn("failed to unmarshal okx FOI message")
		return
	}
	oi, err := strconv.ParseFloat(evt.OI, 64)
	if err != nil {
		log.WithError(err).Warn("invalid okx oi value")
		return
	}
	ts, err := strconv.ParseInt(evt.Ts, 10, 64)
	if err != nil {
		ts = raw.Timestamp.UnixMilli()
	}
	entry := models.NormFOIMessage{
		Symbol:       symbols.ToBinance(raw.Exchange, raw.Symbol),
		EventTime:    ts,
		OpenInterest: oi,
		ReceivedTime: raw.Timestamp.UnixMilli(),
	}
	p.addToBatch(raw, entry)
}

func (p *FOIProcessor) addToBatch(raw models.RawFOIMessage, entry models.NormFOIMessage) {
	normalSymbol := symbols.ToBinance(raw.Exchange, raw.Symbol)
	key := fmt.Sprintf("%s_%s_%s", raw.Exchange, raw.Market, normalSymbol)

	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		if state, ok = p.batches[key]; !ok {
			state = &foiBatchState{
				batch: &models.BatchFOIMessage{
					BatchID:     uuid.New().String(),
					Exchange:    raw.Exchange,
					Symbol:      normalSymbol,
					Market:      raw.Market,
					Entries:     make([]models.NormFOIMessage, 0, p.config.Processor.BatchSize),
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
	b.Entries = append(b.Entries, entry)
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

func (p *FOIProcessor) flusher() {
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

func (p *FOIProcessor) flushTimedOut() {
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

func (p *FOIProcessor) flush(key string) {
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
		state.batch = &models.BatchFOIMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormFOIMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	} else if p.ctx.Err() != nil {
		state.mu.Unlock()
		return
	} else {
		metrics.EmitDropMetric(p.log, metrics.DropMetricOther, batch.Exchange, batch.Market, batch.Symbol, "norm")
		p.log.WithComponent("foi_processor").WithFields(logger.Fields{"batch_key": key}).Warn("normfoi channel full, dropping batch")
		state.batch = &models.BatchFOIMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormFOIMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	}
	state.mu.Unlock()
}

func (p *FOIProcessor) flushAll() {
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

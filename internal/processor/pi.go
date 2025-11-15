package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	appconfig "cryptoflow/config"
	piChannel "cryptoflow/internal/channel/pi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
)

type piBatchState struct {
	mu        sync.Mutex
	batch     *models.BatchPIMessage
	lastFlush time.Time
}

// PIProcessor normalizes premium-index snapshots into batches.
type PIProcessor struct {
	config        *appconfig.Config
	channels      *piChannel.Channels
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
	batches       map[string]*piBatchState
	symbols       map[string]struct{}
	filterSymbols bool
}

// NewPIProcessor creates a processor bound to the PI channels.
func NewPIProcessor(cfg *appconfig.Config, ch *piChannel.Channels) *PIProcessor {
	symSet := make(map[string]struct{})
	if cfg.Source != nil {
		if s := cfg.Source.Binance; s != nil && s.Future != nil && s.Future.PremiumIndex.Enabled {
			for _, x := range s.Future.PremiumIndex.Symbols {
				symSet[x] = struct{}{}
			}
		}
		if s := cfg.Source.Bybit; s != nil && s.Future != nil && s.Future.PremiumIndex.Enabled {
			for _, x := range s.Future.PremiumIndex.Symbols {
				symSet[symbols.ToBinance("bybit", x)] = struct{}{}
			}
		}
		if s := cfg.Source.Kucoin; s != nil && s.Future != nil && s.Future.PremiumIndex.Enabled {
			for _, x := range s.Future.PremiumIndex.Symbols {
				symSet[symbols.ToBinance("kucoin", x)] = struct{}{}
			}
		}
		if s := cfg.Source.Okx; s != nil && s.Future != nil && s.Future.PremiumIndex.Enabled {
			for _, x := range s.Future.PremiumIndex.Symbols {
				symSet[symbols.ToBinance("okx", x)] = struct{}{}
			}
		}
	}

	filter := len(symSet) > 0
	if !filter {
		symSet = make(map[string]struct{})
	}

	return &PIProcessor{
		config:        cfg,
		channels:      ch,
		wg:            &sync.WaitGroup{},
		log:           logger.GetLogger(),
		batches:       make(map[string]*piBatchState),
		symbols:       symSet,
		filterSymbols: filter,
	}
}

// Start begins consuming the raw PI channel.
func (p *PIProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("PI processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	log := p.log.WithComponent("pi_processor").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting PI processor")

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
	return nil
}

// Stop drains buffers and halts workers.
func (p *PIProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("pi_processor").Info("stopping PI processor")
	p.flushAll()
	p.wg.Wait()
	p.log.WithComponent("pi_processor").Info("PI processor stopped")
}

func (p *PIProcessor) worker(id int) {
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
				if _, ok := p.symbols[symbols.ToBinance(msg.Exchange, msg.Symbol)]; !ok {
					continue
				}
			}
			p.handleMessage(msg)
		}
	}
}

func (p *PIProcessor) handleMessage(raw models.RawPIMessage) {
	normalSymbol := symbols.ToBinance(raw.Exchange, raw.Symbol)
	eventTime := raw.Timestamp.UnixMilli()
	if eventTime == 0 {
		eventTime = time.Now().UnixMilli()
	}

	entry := models.NormPIMessage{
		Symbol:               normalSymbol,
		EventTime:            eventTime,
		MarkPrice:            raw.MarkPrice,
		IndexPrice:           raw.IndexPrice,
		EstimatedSettlePrice: raw.EstimatedSettlePrice,
		FundingRate:          raw.FundingRate,
		NextFundingTime:      raw.NextFundingTime.UnixMilli(),
		PremiumIndex:         raw.PremiumIndex,
		ReceivedTime:         time.Now().UnixMilli(),
	}

	p.addToBatch(raw, entry)
}

func (p *PIProcessor) addToBatch(raw models.RawPIMessage, entry models.NormPIMessage) {
	normalSymbol := symbols.ToBinance(raw.Exchange, raw.Symbol)
	key := fmt.Sprintf("%s_%s_%s", raw.Exchange, raw.Market, normalSymbol)

	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		if state, ok = p.batches[key]; !ok {
			state = &piBatchState{
				batch: &models.BatchPIMessage{
					BatchID:     uuid.New().String(),
					Exchange:    raw.Exchange,
					Symbol:      normalSymbol,
					Market:      raw.Market,
					Entries:     make([]models.NormPIMessage, 0, p.config.Processor.BatchSize),
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

func (p *PIProcessor) flusher() {
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

func (p *PIProcessor) flushTimedOut() {
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

func (p *PIProcessor) flush(key string) {
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
		state.batch = &models.BatchPIMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormPIMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	} else if p.ctx.Err() != nil {
		state.mu.Unlock()
		return
	} else {
		metrics.EmitDropMetric(p.log, metrics.DropMetricOther, batch.Exchange, batch.Market, batch.Symbol, "norm")
		p.log.WithComponent("pi_processor").WithFields(logger.Fields{"batch_key": key}).Warn("normpi channel full, dropping batch")
		state.batch = &models.BatchPIMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormPIMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	}
	state.mu.Unlock()
}

func (p *PIProcessor) flushAll() {
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

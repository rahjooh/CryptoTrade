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
	"cryptoflow/logger"
	"cryptoflow/models"
)

// DeltaProcessor normalizes order book delta messages and batches them
// before forwarding to the next stage.
type DeltaProcessor struct {
	config   *appconfig.Config
	rawChan  <-chan models.RawFOBDmodel
	normChan chan<- models.RawFOBDbatchModel
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log

	batches   map[string]*models.RawFOBDbatchModel
	lastFlush map[string]time.Time

	symbols map[string]struct{}
}

// NewDeltaProcessor creates a new processor instance.
func NewDeltaProcessor(cfg *appconfig.Config, rawChan <-chan models.RawFOBDmodel, normChan chan<- models.RawFOBDbatchModel) *DeltaProcessor {
	symSet := make(map[string]struct{})
	for _, s := range cfg.Source.Binance.Future.Orderbook.Delta.Symbols {
		symSet[s] = struct{}{}
	}
	for _, s := range cfg.Source.Kucoin.Future.Orderbook.Delta.Symbols {
		symSet[s] = struct{}{}
	}
	return &DeltaProcessor{
		config:    cfg,
		rawChan:   rawChan,
		normChan:  normChan,
		wg:        &sync.WaitGroup{},
		log:       logger.GetLogger(),
		batches:   make(map[string]*models.RawFOBDbatchModel),
		lastFlush: make(map[string]time.Time),
		symbols:   symSet,
	}
}

// Start begins processing messages from the raw delta channel.
func (p *DeltaProcessor) Start(ctx context.Context) error {
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

	log.Info("delta processor started successfully")
	return nil
}

// Stop signals all workers and flushes remaining batches.
func (p *DeltaProcessor) Stop() {
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("delta_processor").Info("stopping delta processor")
	p.flushAll()
	p.wg.Wait()
	p.log.WithComponent("delta_processor").Info("delta processor stopped")
}

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
			if _, ok := p.symbols[msg.Symbol]; !ok {
				// drop unneeded symbol
				continue
			}
			p.handleMessage(msg)
		}
	}
}

func (p *DeltaProcessor) handleMessage(raw models.RawFOBDmodel) {
	log := p.log.WithComponent("delta_processor").WithFields(logger.Fields{
		"symbol":   raw.Symbol,
		"exchange": raw.Exchange,
	})

	var evt models.BinanceDepthEvent
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		log.WithError(err).Warn("failed to unmarshal delta message")
		return
	}

	entries := make([]models.RawFOBDentryModel, 0, len(evt.Bids)+len(evt.Asks))
	recv := raw.Timestamp.UnixMilli()
	for _, b := range evt.Bids {
		price, err1 := strconv.ParseFloat(b.Price, 64)
		qty, err2 := strconv.ParseFloat(b.Quantity, 64)
		if err1 != nil || err2 != nil || price == 0 || qty == 0 {
			continue
		}
		entries = append(entries, models.RawFOBDentryModel{
			Symbol:        raw.Symbol,
			EventTime:     evt.Time,
			UpdateID:      evt.LastUpdateID,
			PrevUpdateID:  evt.PrevLastUpdateID,
			FirstUpdateID: evt.FirstUpdateID,
			Side:          "bid",
			Price:         price,
			Quantity:      qty,
			ReceivedTime:  recv,
		})
	}
	for _, a := range evt.Asks {
		price, err1 := strconv.ParseFloat(a.Price, 64)
		qty, err2 := strconv.ParseFloat(a.Quantity, 64)
		if err1 != nil || err2 != nil || price == 0 || qty == 0 {
			continue
		}
		entries = append(entries, models.RawFOBDentryModel{
			Symbol:        raw.Symbol,
			EventTime:     evt.Time,
			UpdateID:      evt.LastUpdateID,
			PrevUpdateID:  evt.PrevLastUpdateID,
			FirstUpdateID: evt.FirstUpdateID,
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

func (p *DeltaProcessor) addToBatch(raw models.RawFOBDmodel, entries []models.RawFOBDentryModel) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s_%s_%s", raw.Exchange, raw.Market, raw.Symbol)
	batch, ok := p.batches[key]
	if !ok {
		batch = &models.RawFOBDbatchModel{
			BatchID:     uuid.New().String(),
			Exchange:    raw.Exchange,
			Symbol:      raw.Symbol,
			Market:      raw.Market,
			Entries:     make([]models.RawFOBDentryModel, 0, p.config.Processor.BatchSize),
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

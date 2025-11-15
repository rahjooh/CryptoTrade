package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	appconfig "cryptoflow/config"
	liqchannel "cryptoflow/internal/channel/liq"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
)

type liqBatchState struct {
	mu        sync.Mutex
	batch     *models.BatchLiquidationMessage
	lastFlush time.Time
}

// LiquidationProcessor normalizes raw liquidation payloads and batches them for S3 writers.
type LiquidationProcessor struct {
	config        *appconfig.Config
	channels      *liqchannel.Channels
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
	batches       map[string]*liqBatchState
	symbols       map[string]struct{}
	filterSymbols bool
}

// NewLiquidationProcessor builds the processor instance.
func NewLiquidationProcessor(cfg *appconfig.Config, ch *liqchannel.Channels) *LiquidationProcessor {
	symSet := make(map[string]struct{})
	if cfg.Source != nil {
		if s := cfg.Source.Binance; s != nil && s.Future != nil && s.Future.Liquidation.Enabled {
			for _, x := range s.Future.Liquidation.Symbols {
				symSet[x] = struct{}{}
			}
		}
		if s := cfg.Source.Bybit; s != nil && s.Future != nil && s.Future.Liquidation.Enabled {
			for _, x := range s.Future.Liquidation.Symbols {
				symSet[symbols.ToBinance("bybit", x)] = struct{}{}
			}
		}
		if s := cfg.Source.Kucoin; s != nil && s.Future != nil && s.Future.Liquidation.Enabled {
			for _, x := range s.Future.Liquidation.Symbols {
				symSet[symbols.ToBinance("kucoin", x)] = struct{}{}
			}
		}
		if s := cfg.Source.Okx; s != nil && s.Future != nil && s.Future.Liquidation.Enabled {
			for _, x := range s.Future.Liquidation.Symbols {
				symSet[symbols.ToBinance("okx", x)] = struct{}{}
			}
		}
	}
	filter := len(symSet) > 0
	if !filter {
		symSet = make(map[string]struct{})
	}

	return &LiquidationProcessor{
		config:        cfg,
		channels:      ch,
		wg:            &sync.WaitGroup{},
		log:           logger.GetLogger(),
		batches:       make(map[string]*liqBatchState),
		symbols:       symSet,
		filterSymbols: filter,
	}
}

// Start begins consuming raw liquidation messages.
func (p *LiquidationProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("liquidation processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	log := p.log.WithComponent("liq_processor").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting liquidation processor")

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

// Stop drains buffers and stops workers.
func (p *LiquidationProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("liq_processor").Info("stopping liquidation processor")
	p.flushAll()
	p.wg.Wait()
	p.log.WithComponent("liq_processor").Info("liquidation processor stopped")
}

func (p *LiquidationProcessor) worker(id int) {
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

func (p *LiquidationProcessor) handleMessage(raw models.RawLiquidationMessage) {
	var (
		entry models.NormLiquidationMessage
		ok    bool
	)

	switch raw.Exchange {
	case "binance":
		entry, ok = normalizeBinanceLiq(raw)
	case "bybit":
		entry, ok = normalizeBybitLiq(raw)
	case "kucoin":
		entry, ok = normalizeKucoinLiq(raw)
	case "okx":
		entry, ok = normalizeOkxLiq(raw)
	default:
		p.log.WithComponent("liq_processor").WithFields(logger.Fields{
			"exchange": raw.Exchange,
		}).Debug("unsupported liquidation exchange, dropping message")
		return
	}

	if !ok {
		return
	}

	entry.Exchange = raw.Exchange
	entry.Market = raw.Market
	entry.RawPayload = raw.Data
	if entry.EventTime == 0 {
		entry.EventTime = raw.Timestamp.UnixMilli()
	}
	entry.ReceivedTime = time.Now().UnixMilli()
	p.addToBatch(raw, entry)
}

func (p *LiquidationProcessor) addToBatch(raw models.RawLiquidationMessage, entry models.NormLiquidationMessage) {
	key := fmt.Sprintf("%s_%s_%s", raw.Exchange, raw.Market, entry.Symbol)

	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		if state, ok = p.batches[key]; !ok {
			state = &liqBatchState{
				batch: &models.BatchLiquidationMessage{
					BatchID:     uuid.New().String(),
					Exchange:    raw.Exchange,
					Symbol:      entry.Symbol,
					Market:      raw.Market,
					Entries:     make([]models.NormLiquidationMessage, 0, p.config.Processor.BatchSize),
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

func (p *LiquidationProcessor) flusher() {
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

func (p *LiquidationProcessor) flushTimedOut() {
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

func (p *LiquidationProcessor) flush(key string) {
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
		state.batch = &models.BatchLiquidationMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormLiquidationMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	} else if p.ctx.Err() != nil {
		state.mu.Unlock()
		return
	} else {
		metrics.EmitDropMetric(p.log, metrics.DropMetricOther, batch.Exchange, batch.Market, batch.Symbol, "norm")
		p.log.WithComponent("liq_processor").WithFields(logger.Fields{"batch_key": key}).Warn("normliq channel full, dropping batch")
		state.batch = &models.BatchLiquidationMessage{
			BatchID:     uuid.New().String(),
			Exchange:    batch.Exchange,
			Symbol:      batch.Symbol,
			Market:      batch.Market,
			Entries:     make([]models.NormLiquidationMessage, 0, p.config.Processor.BatchSize),
			ProcessedAt: time.Now(),
		}
		state.lastFlush = time.Now()
	}
	state.mu.Unlock()
}

func (p *LiquidationProcessor) flushAll() {
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

func normalizeBinanceLiq(raw models.RawLiquidationMessage) (models.NormLiquidationMessage, bool) {
	type binanceOrder struct {
		EventTime int64 `json:"E"`
		Order     struct {
			Symbol   string `json:"s"`
			Side     string `json:"S"`
			OrderType string `json:"o"`
			Qty      string `json:"q"`
			Price    string `json:"p"`
		} `json:"o"`
	}
	var evt binanceOrder
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		return models.NormLiquidationMessage{}, false
	}
	price := parseFloat(evt.Order.Price)
	qty := parseFloat(evt.Order.Qty)
	return models.NormLiquidationMessage{
		Symbol:     symbols.ToBinance(raw.Exchange, evt.Order.Symbol),
		Side:       strings.ToUpper(evt.Order.Side),
		OrderType:  strings.ToUpper(evt.Order.OrderType),
		Price:      price,
		Quantity:   qty,
		EventTime:  evt.EventTime,
	}, true
}

func normalizeBybitLiq(raw models.RawLiquidationMessage) (models.NormLiquidationMessage, bool) {
	type bybitData struct {
		Topic string `json:"topic"`
		Ts    int64  `json:"ts"`
		Data  struct {
			Symbol    string `json:"symbol"`
			Side      string `json:"side"`
			Size      string `json:"size"`
			Price     string `json:"price"`
			ExecQty   string `json:"execQty"`
			ExecPrice string `json:"execPrice"`
			Updated   string `json:"updatedTime"`
		} `json:"data"`
	}
	var evt bybitData
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		return models.NormLiquidationMessage{}, false
	}
	price := parseFloat(evt.Data.ExecPrice)
	if price == 0 {
		price = parseFloat(evt.Data.Price)
	}
	qty := parseFloat(evt.Data.ExecQty)
	if qty == 0 {
		qty = parseFloat(evt.Data.Size)
	}
	eventTime := evt.Ts
	if eventTime == 0 {
		eventTime = raw.Timestamp.UnixMilli()
	}
	return models.NormLiquidationMessage{
		Symbol:    symbols.ToBinance(raw.Exchange, evt.Data.Symbol),
		Side:      strings.ToUpper(evt.Data.Side),
		OrderType: "MARKET",
		Price:     price,
		Quantity:  qty,
		EventTime: eventTime,
	}, true
}

func normalizeKucoinLiq(raw models.RawLiquidationMessage) (models.NormLiquidationMessage, bool) {
	type kucoinPayload struct {
		Data struct {
			Symbol string `json:"symbol"`
			Side   string `json:"side"`
			Size   int32  `json:"size"`
			Price  string `json:"price"`
			Ts     int64  `json:"ts"`
		} `json:"data"`
	}
	var evt kucoinPayload
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		return models.NormLiquidationMessage{}, false
	}
	return models.NormLiquidationMessage{
		Symbol:    symbols.ToBinance(raw.Exchange, evt.Data.Symbol),
		Side:      strings.ToUpper(evt.Data.Side),
		OrderType: "MARKET",
		Price:     parseFloat(evt.Data.Price),
		Quantity:  float64(evt.Data.Size),
		EventTime: evt.Data.Ts,
	}, true
}

func normalizeOkxLiq(raw models.RawLiquidationMessage) (models.NormLiquidationMessage, bool) {
	type okxPayload struct {
		Data struct {
			InstID string `json:"instId"`
			Side   string `json:"side"`
			Size   string `json:"sz"`
			Price  string `json:"px"`
			Ts     string `json:"ts"`
		} `json:"data"`
	}
	var evt okxPayload
	if err := json.Unmarshal(raw.Data, &evt); err != nil {
		return models.NormLiquidationMessage{}, false
	}
	eventTime := parseInt64(evt.Data.Ts)
	return models.NormLiquidationMessage{
		Symbol:    symbols.ToBinance(raw.Exchange, evt.Data.InstID),
		Side:      strings.ToUpper(evt.Data.Side),
		OrderType: "MARKET",
		Price:     parseFloat(evt.Data.Price),
		Quantity:  parseFloat(evt.Data.Size),
		EventTime: eventTime,
	}, true
}

func parseFloat(v string) float64 {
	if v == "" {
		return 0
	}
	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return val
}

func parseInt64(v string) int64 {
	if v == "" {
		return 0
	}
	val, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return val
}

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	liqchannel "cryptoflow/internal/channel/liq"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

type Processor struct {
	okxAllowed map[string]bool // uppercase symbol filter; empty => allow all
}

func NewProcessor(okxAllowedSymbols []string) *Processor {
	m := make(map[string]bool)
	for _, s := range okxAllowedSymbols {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		m[strings.ToUpper(s)] = true
	}
	return &Processor{okxAllowed: m}
}

// LiquidationProcessor wires the flattening logic to the raw/norm channels.
type LiquidationProcessor struct {
	config   *appconfig.Config
	channels *liqchannel.Channels
	worker   *Processor
	ctx      context.Context
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
	log      *logger.Log
}

// NewLiquidationProcessor constructs a new processor that drains liq channels.
func NewLiquidationProcessor(cfg *appconfig.Config, ch *liqchannel.Channels) *LiquidationProcessor {
	var symbols []string
	if cfg != nil {
		symbols = cfg.Source.Okx.Future.Liquidation.Symbols
	}
	return &LiquidationProcessor{
		config:   cfg,
		channels: ch,
		worker:   NewProcessor(symbols),
		log:      logger.GetLogger(),
	}
}

// Start begins draining the raw liquidation channel.
func (p *LiquidationProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("liquidation processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	operationLog := p.log.WithComponent("liquidation_processor").WithFields(logger.Fields{
		"operation": "start",
	})
	operationLog.Info("starting liquidation processor")

	workers := 1
	if p.config != nil && p.config.Processor.MaxWorkers > 0 {
		workers = p.config.Processor.MaxWorkers
	}
	operationLog.WithFields(logger.Fields{"workers": workers}).Info("spawning liquidation workers")

	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.workerLoop(ctx, i)
	}

	return nil
}

// Stop waits for workers to exit.
func (p *LiquidationProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("liquidation_processor").Info("stopping liquidation processor")
	p.wg.Wait()
	p.log.WithComponent("liquidation_processor").Info("liquidation processor stopped")
}

func (p *LiquidationProcessor) workerLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()

	logEntry := p.log.WithComponent("liquidation_processor").WithFields(logger.Fields{
		"worker_id": workerID,
		"operation": "worker",
	})
	logEntry.Info("liquidation worker started")

	if p.channels == nil {
		logEntry.Warn("channels not configured, worker exiting")
		return
	}

	p.worker.Run(ctx, p.channels.Raw, p.channels.Norm)
	logEntry.Info("liquidation worker stopped")
}

func (p *Processor) Run(
	ctx context.Context,
	liqRawCh <-chan models.RawLiquidation,
	liqNormCh chan<- models.NormalizedLiquidation,
) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[processor] context canceled, stopping")
			return

		case raw, ok := <-liqRawCh:
			if !ok {
				log.Printf("[processor] liq.raw closed, stopping")
				return
			}

			switch raw.Exchange {
			case models.ExchangeBinance:
				env, err := p.flattenBinance(raw)
				if err != nil {
					log.Printf("[processor] binance flatten error: %v", err)
					continue
				}
				select {
				case liqNormCh <- env:
				case <-ctx.Done():
					return
				}

			case models.ExchangeBybit:
				envs, err := p.flattenBybit(raw)
				if err != nil {
					log.Printf("[processor] bybit flatten error: %v", err)
					continue
				}
				for _, env := range envs {
					select {
					case liqNormCh <- env:
					case <-ctx.Done():
						return
					}
				}

			case models.ExchangeOKX:
				envs, err := p.flattenOKX(raw)
				if err != nil {
					log.Printf("[processor] okx flatten error: %v", err)
					continue
				}
				for _, env := range envs {
					select {
					case liqNormCh <- env:
					case <-ctx.Done():
						return
					}
				}

			default:
				// ignore unknown
			}
		}
	}
}

// ---------------- BINANCE ----------------

func (p *Processor) flattenBinance(raw models.RawLiquidation) (models.NormalizedLiquidation, error) {
	var evt models.BinanceLiquidationEvent
	if err := json.Unmarshal(raw.Payload, &evt); err != nil {
		return models.NormalizedLiquidation{}, err
	}

	o := evt.Order

	parseF := func(s string) float64 {
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}

	ts := time.UnixMilli(evt.EventTime)
	if evt.EventTime == 0 && o.TradeTime != 0 {
		ts = time.UnixMilli(o.TradeTime)
	}

	b := &models.BinanceNormalizedLiquidation{
		Symbol:       o.Symbol,
		Side:         o.Side,
		PositionSide: o.PositionSide,
		OrderType:    o.OrderType,
		Quantity:     parseF(o.QuantityStr),
		Price:        parseF(o.PriceStr),
		AvgPrice:     parseF(o.AvgPriceStr),
		LastQty:      parseF(o.LastQtyStr),
		LastPrice:    parseF(o.LastPriceStr),
		TradeID:      o.TradeID,
		IsMaker:      o.Maker,
		IsReduceOnly: o.ReduceOnly,
		WorkingType:  o.WorkingType,
		OriginalType: o.OriginalType,
		CloseAll:     o.CloseAll,
		RealizedPnl:  parseF(o.RealizedPnl),
	}

	return models.NormalizedLiquidation{
		Exchange: models.ExchangeBinance,
		Time:     ts,
		Binance:  b,
	}, nil
}

// ---------------- BYBIT ----------------

func (p *Processor) flattenBybit(raw models.RawLiquidation) ([]models.NormalizedLiquidation, error) {
	var evt models.BybitLiquidationEvent
	if err := json.Unmarshal(raw.Payload, &evt); err != nil {
		return nil, err
	}

	parseF := func(s string) float64 {
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}

	sideNorm := func(s string) string {
		switch strings.ToLower(s) {
		case "buy":
			return "BUY"
		case "sell":
			return "SELL"
		default:
			return strings.ToUpper(s)
		}
	}

	out := make([]models.NormalizedLiquidation, 0, len(evt.Data))
	for _, d := range evt.Data {
		ts := time.UnixMilli(d.EventTime)

		b := &models.BybitNormalizedLiquidation{
			Symbol:   d.Symbol,
			Side:     sideNorm(d.Side),
			Quantity: parseF(d.SizeStr),
			Price:    parseF(d.PriceStr),
		}

		env := models.NormalizedLiquidation{
			Exchange: models.ExchangeBybit,
			Time:     ts,
			Bybit:    b,
		}
		out = append(out, env)
	}

	return out, nil
}

// ---------------- OKX ----------------

func (p *Processor) flattenOKX(raw models.RawLiquidation) ([]models.NormalizedLiquidation, error) {
	var evt models.OKXLiquidationEvent
	if err := json.Unmarshal(raw.Payload, &evt); err != nil {
		return nil, err
	}

	parseF := func(s string) float64 {
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}

	sideNorm := func(s string) string {
		switch strings.ToLower(s) {
		case "buy":
			return "BUY"
		case "sell":
			return "SELL"
		default:
			return strings.ToUpper(s)
		}
	}

	posSideNorm := func(s string) string {
		switch strings.ToLower(s) {
		case "long":
			return "LONG"
		case "short":
			return "SHORT"
		default:
			return strings.ToUpper(s)
		}
	}

	var out []models.NormalizedLiquidation

	for _, block := range evt.Data {
		symbol := strings.ToUpper(block.InstFamily)
		if symbol == "" {
			symbol = strings.ToUpper(block.Uly)
		}

		// Symbol filter
		if len(p.okxAllowed) > 0 && !p.okxAllowed[symbol] {
			continue
		}

		for _, d := range block.Details {
			tsMs, err := strconv.ParseInt(d.Ts, 10, 64)
			if err != nil {
				tsMs = 0
			}
			ts := time.UnixMilli(tsMs)

			o := &models.OKXNormalizedLiquidation{
				Symbol:       symbol,
				Side:         sideNorm(d.Side),
				PositionSide: posSideNorm(d.PosSide),
				Quantity:     parseF(d.Sz),
				Price:        parseF(d.BkPx),
			}

			env := models.NormalizedLiquidation{
				Exchange: models.ExchangeOKX,
				Time:     ts,
				OKX:      o,
			}
			out = append(out, env)
		}
	}

	return out, nil
}

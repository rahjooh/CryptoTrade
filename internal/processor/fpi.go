package processor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fpichannel "cryptoflow/internal/channel/fpi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
)

// FPIProcessor normalizes premium-index raw messages into a flat schema.
type FPIProcessor struct {
	config   *appconfig.Config
	channels *fpichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	log      *logger.Log

	running bool

	symbols       map[string]struct{}
	filterSymbols bool
}

// NewFPIProcessor initialises a processor instance.
func NewFPIProcessor(cfg *appconfig.Config, ch *fpichannel.Channels) *FPIProcessor {
	symbolSet := make(map[string]struct{})

	if cfg.Source.Binance.Future.FPI.Enabled {
		for _, sym := range cfg.Source.Binance.Future.FPI.Symbols {
			symbolSet[symbols.ToBinance(models.ExchangeBinance, sym)] = struct{}{}
		}
	}
	if cfg.Source.Bybit.Future.FPI.Enabled {
		for _, sym := range cfg.Source.Bybit.Future.FPI.Symbols {
			symbolSet[symbols.ToBinance(models.ExchangeBybit, sym)] = struct{}{}
		}
	}
	if cfg.Source.Kucoin.Future.FPI.Enabled {
		for _, sym := range cfg.Source.Kucoin.Future.FPI.Symbols {
			symbolSet[symbols.ToBinance(models.ExchangeKucoin, sym)] = struct{}{}
		}
	}
	if cfg.Source.Okx.Future.FPI.Enabled {
		for _, sym := range cfg.Source.Okx.Future.FPI.Symbols {
			symbolSet[symbols.ToBinance(models.ExchangeOKX, sym)] = struct{}{}
		}
	}

	filter := len(symbolSet) > 0
	if !filter {
		symbolSet = make(map[string]struct{})
	}

	return &FPIProcessor{
		config:        cfg,
		channels:      ch,
		wg:            &sync.WaitGroup{},
		log:           logger.GetLogger(),
		symbols:       symbolSet,
		filterSymbols: filter,
	}
}

// Start spins up worker goroutines consuming fpi.raw.
func (p *FPIProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("fpi processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	log := p.log.WithComponent("fpi_processor").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting FPI processor")

	workers := p.config.Processor.MaxWorkers
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	return nil
}

// Stop drains channels and stops workers.
func (p *FPIProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("fpi_processor").Info("stopping FPI processor")
	p.wg.Wait()
	p.log.WithComponent("fpi_processor").Info("FPI processor stopped")
}

func (p *FPIProcessor) worker(id int) {
	defer p.wg.Done()
	log := p.log.WithComponent("fpi_worker").WithFields(logger.Fields{"worker_id": id})

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context canceled; stopping fpi worker")
			return
		case raw, ok := <-p.channels.Raw:
			if !ok {
				log.Info("fpi.raw channel closed; worker exiting")
				return
			}
			p.handle(raw)
		}
	}
}

func (p *FPIProcessor) handle(raw models.RawFPI) {
	normalSymbol := symbols.ToBinance(raw.Exchange, raw.Symbol)
	if normalSymbol == "" {
		normalSymbol = strings.ToUpper(raw.Symbol)
	}

	if p.filterSymbols {
		if _, ok := p.symbols[normalSymbol]; !ok {
			return
		}
	}

	eventTime := raw.EventTime
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}

	nextFunding := raw.NextFundingTime
	market := raw.Market
	if market == "" {
		market = "fpi"
	}

	env := models.NormFPI{
		Exchange:             raw.Exchange,
		Market:               market,
		Symbol:               normalSymbol,
		EventTimeMs:          eventTime.UnixMilli(),
		MarkPrice:            raw.MarkPrice,
		IndexPrice:           raw.IndexPrice,
		EstimatedSettlePrice: raw.EstimatedSettlePrice,
		FundingRate:          raw.FundingRate,
		NextFundingTimeMs:    nextFunding.UnixMilli(),
		PremiumIndex:         raw.PremiumIndex,
		ReceivedTimeMs:       time.Now().UnixMilli(),
		Source:               raw.Source,
	}

	if !p.channels.SendNorm(p.ctx, env) {
		if p.ctx.Err() != nil {
			return
		}
		metrics.EmitDropMetric(p.log, metrics.DropMetricPremiumIndexRaw, raw.Exchange, market, raw.Symbol, "norm")
	}
}

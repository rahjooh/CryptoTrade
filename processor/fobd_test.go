package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appconfig "cryptoflow/config"
	fobdchan "cryptoflow/internal/channel/fobd"
	"cryptoflow/models"
)

func minimalDeltaConfig() *appconfig.Config {
	return &appconfig.Config{
		Processor: appconfig.ProcessorConfig{
			MaxWorkers:   1,
			BatchSize:    1,
			BatchTimeout: time.Millisecond,
		},
		Source: appconfig.SourceConfig{
			Binance: appconfig.BinanceSourceConfig{
				Future: appconfig.BinanceFutureConfig{
					Orderbook: appconfig.BinanceFutureOrderbookConfig{
						Delta: appconfig.BinanceDeltaConfig{Symbols: []string{}},
					},
				},
			},
			Kucoin: appconfig.KucoinSourceConfig{
				Future: appconfig.KucoinFutureConfig{
					Orderbook: appconfig.KucoinFutureOrderbookConfig{
						Delta: appconfig.KucoinDeltaConfig{Symbols: []string{}},
					},
				},
			},
		},
	}
}

func TestDeltaProcessorStartStop(t *testing.T) {
	cfg := minimalDeltaConfig()
	ch := fobdchan.NewChannels(1, 1)
	p := NewDeltaProcessor(cfg, ch)
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := p.Start(ctx); err == nil {
		t.Fatalf("expected error on second start")
	}
	cancel()
	p.Stop()
}

func TestDeltaProcessorNormalizesSymbols(t *testing.T) {
	cfg := minimalDeltaConfig()
	cfg.Processor.BatchSize = 2
	ch := fobdchan.NewChannels(1, 1)
	p := NewDeltaProcessor(cfg, ch)

	evt := models.BinanceFOBDResp{
		Time:             1,
		LastUpdateID:     1,
		PrevLastUpdateID: 0,
		FirstUpdateID:    1,
		Bids:             []models.FOBDEntry{{Price: "1", Quantity: "1"}},
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	raw := models.RawFOBDMessage{
		Exchange:  "binance",
		Symbol:    "1000BONKUSDT",
		Market:    "fobd",
		Data:      data,
		Timestamp: time.Now(),
	}

	p.handleMessage(raw)

	key := "binance_fobd_BONKUSDT"
	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		t.Fatalf("expected batch key %s", key)
	}
	state.mu.Lock()
	batch := state.batch
	state.mu.Unlock()
	if batch.Symbol != "BONKUSDT" {
		t.Fatalf("expected batch symbol BONKUSDT, got %s", batch.Symbol)
	}
	if len(batch.Entries) == 0 || batch.Entries[0].Symbol != "BONKUSDT" {
		t.Fatalf("expected entry symbol BONKUSDT, got %+v", batch.Entries)
	}
}

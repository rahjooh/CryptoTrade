package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appconfig "cryptoflow/config"
	fobdchan "cryptoflow/internal/channel/fobd"
	fobschan "cryptoflow/internal/channel/fobs"
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

func minimalConfig() *appconfig.Config {
	return &appconfig.Config{
		Processor: appconfig.ProcessorConfig{
			MaxWorkers:   1,
			BatchSize:    1,
			BatchTimeout: time.Millisecond,
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
		Bids: []models.FOBDEntry{{
			Price:    "1",
			Quantity: "1",
		}},
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

func TestFlattenerStartStop(t *testing.T) {
	cfg := minimalConfig()
	ch := fobschan.NewChannels(1, 1)
	f := NewFlattener(cfg, ch)
	ctx, cancel := context.WithCancel(context.Background())
	if err := f.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := f.Start(ctx); err == nil {
		t.Fatalf("expected error on second start")
	}
	cancel()
	f.Stop()
}

func TestFlattenerNormalizesSymbols(t *testing.T) {
	cfg := minimalConfig()
	cfg.Processor.BatchSize = 2
	ch := fobschan.NewChannels(1, 1)
	f := NewFlattener(cfg, ch)

	rawMsg := models.RawFOBSMessage{
		Exchange:  "binance",
		Symbol:    "1000BONKUSDT",
		Market:    "future-orderbook-snapshot",
		Timestamp: time.Now(),
	}
	ob := models.BinanceFOBSresp{
		LastUpdateID: 1,
		Bids:         [][]string{{"1", "1"}},
	}

	entries := f.flattenOrderbook(rawMsg, ob)
	if len(entries) == 0 || entries[0].Symbol != "BONKUSDT" {
		t.Fatalf("expected normalized symbol BONKUSDT, got %v", entries)
	}

	f.addToBatch(rawMsg, entries)
	key := "binance_future-orderbook-snapshot_BONKUSDT"
	f.mu.RLock()
	batch, ok := f.batches[key]
	f.mu.RUnlock()
	if !ok {
		t.Fatalf("expected batch key %s", key)
	}
	if batch.Symbol != "BONKUSDT" {
		t.Fatalf("expected batch symbol BONKUSDT, got %s", batch.Symbol)
	}
}

func TestDeltaProcessorHandlesBybit(t *testing.T) {
	cfg := minimalDeltaConfig()
	cfg.Processor.BatchSize = 2
	cfg.Source.Bybit = appconfig.BybitSourceConfig{
		Future: appconfig.BybitFutureConfig{
			Orderbook: appconfig.BybitFutureOrderbookConfig{
				Delta: appconfig.BybitDeltaConfig{Symbols: []string{"BTCUSDT"}},
			},
		},
	}
	ch := fobdchan.NewChannels(1, 1)
	p := NewDeltaProcessor(cfg, ch)
	p.ctx = context.Background()

	evt := models.BybitFOBDResp{
		Ts: 1,
	}
	evt.Data.Symbol = "BTCUSDT"
	evt.Data.Bids = [][]string{{"1", "1"}}
	evt.Data.Seq = 1
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	raw := models.RawFOBDMessage{
		Exchange:  "bybit",
		Symbol:    "BTCUSDT",
		Market:    "fobd",
		Data:      data,
		Timestamp: time.Now(),
	}

	p.handleMessage(raw)

	key := "bybit_fobd_BTCUSDT"
	p.mu.RLock()
	state, ok := p.batches[key]
	p.mu.RUnlock()
	if !ok {
		t.Fatalf("expected batch key %s", key)
	}
	state.mu.Lock()
	batch := state.batch
	state.mu.Unlock()
	if batch.RecordCount != 1 {
		t.Fatalf("expected 1 record, got %d", batch.RecordCount)
	}
	if batch.Symbol != "BTCUSDT" {
		t.Fatalf("expected symbol BTCUSDT, got %s", batch.Symbol)
	}
}

func TestFlattenerProcessesBybitSnapshot(t *testing.T) {
	cfg := minimalConfig()
	cfg.Processor.BatchSize = 2
	ch := fobschan.NewChannels(1, 1)
	f := NewFlattener(cfg, ch)
	f.ctx = context.Background()

	ob := models.BybitFOBSresp{
		UpdateID: 1,
		Bids:     [][]string{{"1", "1"}},
	}
	data, err := json.Marshal(ob)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	raw := models.RawFOBSMessage{
		Exchange:  "bybit",
		Symbol:    "BTCUSDT",
		Market:    "future-orderbook-snapshot",
		Data:      data,
		Timestamp: time.Now(),
	}

	count := f.processMessage(raw)
	if count != 1 {
		t.Fatalf("expected 1 entry, got %d", count)
	}

	key := "bybit_future-orderbook-snapshot_BTCUSDT"
	f.mu.RLock()
	batch, ok := f.batches[key]
	f.mu.RUnlock()
	if !ok {
		t.Fatalf("expected batch key %s", key)
	}
	if batch.RecordCount != 1 {
		t.Fatalf("expected 1 record, got %d", batch.RecordCount)
	}
}

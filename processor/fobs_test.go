package processor

import (
	"context"
	"testing"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/models"
)

func minimalConfig() *appconfig.Config {
	return &appconfig.Config{
		Processor: appconfig.ProcessorConfig{
			MaxWorkers:   1,
			BatchSize:    1,
			BatchTimeout: time.Millisecond,
		},
	}
}

func TestFlattenerStartStop(t *testing.T) {
	cfg := minimalConfig()
	raw := make(chan models.RawFOBSMessage)
	norm := make(chan models.BatchFOBSMessage)
	f := NewFlattener(cfg, raw, norm)
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
	raw := make(chan models.RawFOBSMessage)
	norm := make(chan models.BatchFOBSMessage)
	f := NewFlattener(cfg, raw, norm)

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

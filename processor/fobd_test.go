package processor

import (
	"context"
	"testing"
	"time"

	appconfig "cryptoflow/config"
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
	raw := make(chan models.RawFOBDMessage)
	norm := make(chan models.BatchFOBDMessage)
	p := NewDeltaProcessor(cfg, raw, norm)
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

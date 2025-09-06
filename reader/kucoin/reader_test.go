package kucoin

import (
	"testing"
	"time"

	"cryptoflow/config"
	"cryptoflow/models"
)

func minimalConfig() *config.Config {
	return &config.Config{
		Reader: config.ReaderConfig{Timeout: time.Second},
		Source: config.SourceConfig{
			Kucoin: config.KucoinSourceConfig{
				ConnectionPool: config.ConnectionPoolConfig{
					MaxIdleConns:    1,
					MaxConnsPerHost: 1,
					IdleConnTimeout: time.Second,
				},
				Future: config.KucoinFutureConfig{
					Orderbook: config.KucoinFutureOrderbookConfig{
						Snapshots: config.KucoinSnapshotConfig{
							Enabled:    true,
							URL:        "https://example.com",
							Limit:      100,
							IntervalMs: 1000,
						},
						Delta: config.KucoinDeltaConfig{
							Enabled:    true,
							URL:        "wss://example.com/ws",
							IntervalMs: 100,
							Symbols:    []string{"XBT-USDTM"},
						},
					},
				},
			},
		},
	}
}

func TestNewReaders(t *testing.T) {
	cfg := minimalConfig()
	fobsCh := make(chan models.RawFOBSMessage)
	r1 := Kucoin_FOBS_NewReader(cfg, fobsCh, []string{"XBT-USDTM"}, "")
	if r1 == nil {
		t.Fatal("Kucoin_FOBS_NewReader returned nil")
	}
}

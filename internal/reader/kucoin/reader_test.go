package kucoin

import (
	"testing"
	"time"

	"cryptoflow/config"
	fobdchan "cryptoflow/internal/channel/fobd"
	fobschan "cryptoflow/internal/channel/fobs"
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
	fobsCh := fobschan.NewChannels(1, 1)
	r1 := Kucoin_FOBS_NewReader(cfg, fobsCh, []string{"XBT-USDTM"}, "")
	if r1 == nil {
		t.Fatal("Kucoin_FOBS_NewReader returned nil")
	}
	fobdCh := fobdchan.NewChannels(1, 1)
	r2 := Kucoin_FOBD_NewReader(cfg, fobdCh, []string{"XBT-USDTM"}, "")
	if r2 == nil {
		t.Fatal("Kucoin_FOBD_NewReader returned nil")
	}
}

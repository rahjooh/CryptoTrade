package binance

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
			Binance: config.BinanceSourceConfig{
				ConnectionPool: config.ConnectionPoolConfig{
					MaxIdleConns:    1,
					MaxConnsPerHost: 1,
					IdleConnTimeout: time.Second,
				},
				Future: config.BinanceFutureConfig{
					Orderbook: config.BinanceFutureOrderbookConfig{
						Snapshots: config.BinanceSnapshotConfig{
							Enabled:    true,
							URL:        "https://example.com",
							Limit:      10,
							IntervalMs: 1000,
						},
						Delta: config.BinanceDeltaConfig{
							Enabled:    true,
							URL:        "wss://example.com/ws",
							IntervalMs: 100,
							Symbols:    []string{"BTCUSDT"},
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
	r1 := Binance_FOBS_NewReader(cfg, fobsCh, []string{"BTCUSDT"}, "")
	if r1 == nil {
		t.Fatal("Binance_FOBS_NewReader returned nil")
	}
	fobdCh := fobdchan.NewChannels(1, 1)
	r2 := Binance_FOBD_NewReader(cfg, fobdCh, []string{"BTCUSDT"}, "")
	if r2 == nil {
		t.Fatal("Binance_FOBD_NewReader returned nil")
	}
}

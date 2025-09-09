package bybit

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
			Bybit: config.BybitSourceConfig{
				ConnectionPool: config.ConnectionPoolConfig{
					MaxIdleConns:    1,
					MaxConnsPerHost: 1,
					IdleConnTimeout: time.Second,
				},
				Future: config.BybitFutureConfig{
					Orderbook: config.BybitFutureOrderbookConfig{
						Snapshots: config.BybitSnapshotConfig{
							Enabled:    true,
							URL:        "https://example.com",
							Limit:      50,
							IntervalMs: 1000,
							Symbols:    []string{"BTCUSDT"},
						},
						Delta: config.BybitDeltaConfig{
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
	r1 := Bybit_FOBS_NewReader(cfg, fobsCh, []string{"BTCUSDT"}, "")
	if r1 == nil {
		t.Fatal("Bybit_FOBS_NewReader returned nil")
	}
	fobdCh := fobdchan.NewChannels(1, 1)
	r2 := Bybit_FOBD_NewReader(cfg, fobdCh, []string{"BTCUSDT"}, "")
	if r2 == nil {
		t.Fatal("Bybit_FOBD_NewReader returned nil")
	}
}

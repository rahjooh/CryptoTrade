package okx

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
			Okx: config.OkxSourceConfig{
				ConnectionPool: config.ConnectionPoolConfig{MaxIdleConns: 1, MaxConnsPerHost: 1, IdleConnTimeout: time.Second},
				Future: config.OkxFutureConfig{
					Orderbook: config.OkxFutureOrderbookConfig{
						Snapshots: config.OkxSnapshotConfig{
							Enabled:    true,
							URL:        "https://example.com",
							Limit:      100,
							IntervalMs: 1000,
						},
						Delta: config.OkxDeltaConfig{
							Enabled:    true,
							URL:        "wss://example.com/ws",
							IntervalMs: 100,
							Symbols:    []string{"BTC-USDT-SWAP"},
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
	if r := Okx_FOBS_NewReader(cfg, fobsCh, []string{"BTC-USDT-SWAP"}, ""); r == nil {
		t.Fatal("Okx_FOBS_NewReader returned nil")
	}
	fobdCh := fobdchan.NewChannels(1, 1)
	if r := Okx_FOBD_NewReader(cfg, fobdCh, []string{"BTC-USDT-SWAP"}, ""); r == nil {
		t.Fatal("Okx_FOBD_NewReader returned nil")
	}
}

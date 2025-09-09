package config

import (
	"os"
	"testing"
)

// writeTempConfig creates a minimal configuration file required for LoadConfig
// and returns its path.
func writeTempConfig(t *testing.T) string {
	t.Helper()
	content := `cryptoflow:
  name: "TestApp"
  version: "1.0"
channels:
  raw_buffer: 1
  processed_buffer: 1
  error_buffer: 1
  pool_size: 1
reader:
  max_workers: 1
processor:
  max_workers: 1
  batch_size: 1
  batch_timeout: 1s
writer:
  buffer:
    snapshot_flush_interval: 1s
    delta_flush_interval: 1s
storage:
  s3:
    enabled: false
`
	f, err := os.CreateTemp("", "cfg-*.yml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return f.Name()
}

func TestLoadConfig(t *testing.T) {
	path := writeTempConfig(t)
	defer os.Remove(path)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Cryptoflow.Name != "TestApp" {
		t.Errorf("unexpected name: %s", cfg.Cryptoflow.Name)
	}
	if cfg.Reader.MaxWorkers != 1 {
		t.Errorf("unexpected max workers: %d", cfg.Reader.MaxWorkers)
	}
}

func TestLoadIPShards(t *testing.T) {
	content := `shards:
- ip: "1.1.1.1"
  binance_symbols: ["BTCUSDT"]
  kucoin_symbols: ["XBTUSDT"]
  okx_symbols:
    swap_orderbook_snapshot: ["BTC-USDT-SWAP"]
    swap_orderbook_delta: ["BTC-USDT"]
`
	f, err := os.CreateTemp("", "shards-*.yml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	defer os.Remove(f.Name())

	shards, err := LoadIPShards(f.Name())
	if err != nil {
		t.Fatalf("LoadIPShards failed: %v", err)
	}
	if len(shards.Shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(shards.Shards))
	}
	if shards.Shards[0].IP != "1.1.1.1" {
		t.Errorf("unexpected IP: %s", shards.Shards[0].IP)
	}
	if len(shards.Shards[0].OkxSymbols.SwapOrderbookSnapshot) != 1 || shards.Shards[0].OkxSymbols.SwapOrderbookSnapshot[0] != "BTC-USDT-SWAP" {
		t.Errorf("unexpected swap_orderbook_snapshot symbols: %v", shards.Shards[0].OkxSymbols.SwapOrderbookSnapshot)
	}
	if len(shards.Shards[0].OkxSymbols.SwapOrderbookDelta) != 1 || shards.Shards[0].OkxSymbols.SwapOrderbookDelta[0] != "BTC-USDT" {
		t.Errorf("unexpected swap_orderbook_delta symbols: %v", shards.Shards[0].OkxSymbols.SwapOrderbookDelta)
	}
}

func TestIsValidS3Bucket(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"valid-bucket", true},
		{"Invalid", false},
		{"ab", false},
		{"my..bucket", false},
	}
	for _, c := range cases {
		if got := isValidS3Bucket(c.name); got != c.valid {
			t.Errorf("isValidS3Bucket(%q) = %v, want %v", c.name, got, c.valid)
		}
	}
}

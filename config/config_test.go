package config

import (
	"fmt"
	"os"
	"testing"
)

// writeTempConfig creates a minimal configuration file required for LoadConfig
// and returns its path.
func writeTempConfig(t *testing.T) string {
	return writeTempConfigWithName(t, "TestApp")
}

func writeTempConfigWithName(t *testing.T, name string) string {
	t.Helper()
	content := fmt.Sprintf(`cryptoflow:
  name: "%s"
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
`, name)
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
	if !cfg.Metrics.UsedWeight {
		t.Errorf("expected used_weight metrics to default to enabled")
	}
	if !cfg.Metrics.ChannelSize {
		t.Errorf("expected channel_size metrics to default to enabled")
	}
}

func TestLoadConfig_DisableMetrics(t *testing.T) {
	content := `cryptoflow:
  name: "TestApp"
  version: "1.0"
metrics:
  used_weight: false
  channel_size: true
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

	f, err := os.CreateTemp("", "cfg-metrics-*.yml")
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

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Metrics.UsedWeight {
		t.Errorf("expected used_weight metrics to be disabled")
	}
	if !cfg.Metrics.ChannelSize {
		t.Errorf("expected channel_size metrics to remain enabled")
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

func TestLoadConfig_UsesEnvironmentSpecificFile(t *testing.T) {
	t.Setenv(appEnvVar, environmentProduction)
	path := writeTempConfigWithName(t, "ProdApp")
	original := configEnvPaths
	configEnvPaths = map[string]string{
		environmentProduction: path,
	}
	t.Cleanup(func() { configEnvPaths = original })
	cfg, err := LoadConfig(defaultConfigPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Cryptoflow.Name != "ProdApp" {
		t.Fatalf("expected production config to load, got %s", cfg.Cryptoflow.Name)
	}
}

func TestLoadConfig_EnvironmentAliases(t *testing.T) {
	path := writeTempConfigWithName(t, "AliasApp")
	original := configEnvPaths
	configEnvPaths = map[string]string{
		environmentProduction: path,
	}
	t.Cleanup(func() { configEnvPaths = original })

	cases := []struct {
		name string
		env  string
	}{
		{name: "prod", env: "prod"},
		{name: "producation", env: "producation"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(appEnvVar, tc.env)
			cfg, err := LoadConfig(defaultConfigPath)
			if err != nil {
				t.Fatalf("LoadConfig failed: %v", err)
			}
			if cfg.Cryptoflow.Name != "AliasApp" {
				t.Fatalf("expected alias %s to resolve production config", tc.env)
			}
		})
	}
}

func TestLoadIPShards_UsesEnvironmentSpecificFile(t *testing.T) {
	t.Setenv(appEnvVar, environmentStaging)
	content := `shards:
- ip: "2.2.2.2"
  binance_symbols: ["ETHUSDT"]
  kucoin_symbols: ["ETHUSDTM"]
  okx_symbols:
    swap_orderbook_snapshot: ["ETH-USDT-SWAP"]
    swap_orderbook_delta: ["ETH-USDT"]
`
	f, err := os.CreateTemp("", "shards-env-*.yml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	original := ipShardsEnvPaths
	ipShardsEnvPaths = map[string]string{
		environmentStaging: f.Name(),
	}
	t.Cleanup(func() { ipShardsEnvPaths = original })
	shards, err := LoadIPShards(defaultIPShardsPath)
	if err != nil {
		t.Fatalf("LoadIPShards failed: %v", err)
	}
	if len(shards.Shards) != 1 || shards.Shards[0].IP != "2.2.2.2" {
		t.Fatalf("expected staging shard configuration to be used")
	}
}

func TestLoadIPShards_EnvironmentAliases(t *testing.T) {
	content := `shards:
- ip: "3.3.3.3"
  binance_symbols: ["LTCUSDT"]
  kucoin_symbols: ["LTCUSDTM"]
  okx_symbols:
    swap_orderbook_snapshot: ["LTC-USDT-SWAP"]
    swap_orderbook_delta: ["LTC-USDT"]
`
	f, err := os.CreateTemp("", "shards-alias-*.yml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })

	original := ipShardsEnvPaths
	ipShardsEnvPaths = map[string]string{
		environmentStaging: f.Name(),
	}
	t.Cleanup(func() { ipShardsEnvPaths = original })

	cases := []struct {
		name string
		env  string
	}{
		{name: "stag", env: "stag"},
		{name: "stagging", env: "stagging"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(appEnvVar, tc.env)
			shards, err := LoadIPShards(defaultIPShardsPath)
			if err != nil {
				t.Fatalf("LoadIPShards failed: %v", err)
			}
			if len(shards.Shards) != 1 || shards.Shards[0].IP != "3.3.3.3" {
				t.Fatalf("expected alias %s to resolve staging shard configuration", tc.env)
			}
		})
	}
}

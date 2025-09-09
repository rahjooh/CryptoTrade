package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

// OkxSymbolSet groups OKX swap order book symbols for snapshot and delta streams.
type OkxSymbolSet struct {
	SwapOrderbookSnapshot []string `yaml:"swap_orderbook_snapshot"`
	SwapOrderbookDelta    []string `yaml:"swap_orderbook_delta"`
}

// IPShard defines a set of symbols that should be fetched using a specific source IP.
// BinanceSymbols, BybitSymbols, KucoinSymbols and OkxSymbols allow specifying exchange specific representations.
type IPShard struct {
	IP             string       `yaml:"ip"`
	BinanceSymbols []string     `yaml:"binance_symbols"`
	BybitSymbols   []string     `yaml:"bybit_symbols"`
	KucoinSymbols  []string     `yaml:"kucoin_symbols"`
	OkxSymbols     OkxSymbolSet `yaml:"okx_symbols"`
}

// IPShards represents the full shard configuration.
type IPShards struct {
	Shards []IPShard `yaml:"shards"`
}

// LoadIPShards loads shard configuration from the given path.
func LoadIPShards(path string) (*IPShards, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read shards file: %w", err)
	}
	var cfg IPShards
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse shards file: %w", err)
	}
	for i := range cfg.Shards {
		cfg.Shards[i].OkxSymbols.SwapOrderbookSnapshot = cfg.Shards[i].OkxSymbols.SwapOrderbookSnapshot
		cfg.Shards[i].OkxSymbols.SwapOrderbookDelta = cfg.Shards[i].OkxSymbols.SwapOrderbookDelta
	}
	return &cfg, nil
}

package config

import (
	"fmt"
	"os"
)

// IPShard defines a set of symbols that should be fetched using a specific source IP.
// BinanceSymbols and KucoinSymbols allow specifying exchange specific representations.
type IPShard struct {
	IP             string   `yaml:"ip"`
	BinanceSymbols []string `yaml:"binance_symbols"`
	KucoinSymbols  []string `yaml:"kucoin_symbols"`
	OkxSymbols     []string `yaml:"okx_symbols"`
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
	return &cfg, nil
}

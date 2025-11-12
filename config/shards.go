package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

const (
	defaultIPShardsPath    = "config/ip_shards.yml"
	productionIPShardsPath = "config/ip_shards.production.yml"
	stagingIPShardsPath    = "config/ip_shards.staging.yml"
)

var ipShardsEnvPaths = map[string]string{
	environmentProduction: productionIPShardsPath,
	environmentStaging:    stagingIPShardsPath,
}

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
	shardPath := resolveEnvSpecificPath(path, defaultIPShardsPath, ipShardsEnvPaths)
	data, err := os.ReadFile(shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read shards file %q: %w", shardPath, err)
	}
	var cfg IPShards
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse shards file: %w", err)
	}
	//for i := range cfg.Shards {
	//	cfg.Shards[i].OkxSymbols.SwapOrderbookSnapshot = cfg.Shards[i].OkxSymbols.SwapOrderbookSnapshot
	//	cfg.Shards[i].OkxSymbols.SwapOrderbookDelta = cfg.Shards[i].OkxSymbols.SwapOrderbookDelta
	//}
	return &cfg, nil
}

// FilterByIP returns only the shards that match one of the provided IPv4
// addresses. The search is case-sensitive because IP addresses are normalised
// strings. The order of the returned shards matches the original configuration
// to keep deterministic reader creation.
func (s *IPShards) FilterByIP(ips []string) []IPShard {
	if len(ips) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		if ip == "" {
			continue
		}
		allowed[ip] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	filtered := make([]IPShard, 0, len(s.Shards))
	for _, shard := range s.Shards {
		if _, ok := allowed[shard.IP]; ok {
			filtered = append(filtered, shard)
		}
	}
	return filtered
}

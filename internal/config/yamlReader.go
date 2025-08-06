// internal/config/yamlReader.go
package config

import (
	"fmt"
	"os"
)

func init() {
	_ = yaml.NewDecoder // This line is usually for preventing unused import errors, can be removed if not needed.
}

// SourceConf holds the top-level structure of the entire application configuration.
type SourceConf struct {
	Source map[string]ExchangeConf `yaml:"source"`
}

// ExchangeConf holds the configuration for a single exchange.
type ExchangeConf struct {
	Spot   *MarketConf `yaml:"spot,omitempty"`
	Future *MarketConf `yaml:"future,omitempty"`
}

// MarketConf holds the configuration for a specific market type (e.g., spot, future).
type MarketConf struct {
	Orderbook    *OrderbookConf `yaml:"orderbook,omitempty"`
	Delta        *Configs       `yaml:"delta,omitempty"`
	Contracts    *Configs       `yaml:"contracts,omitempty"`
	Liquidation  *Configs       `yaml:"liquidation,omitempty"`
	OpenInterest *Configs       `yaml:"open_interest,omitempty"`
	FundingRate  *Configs       `yaml:"funding_rate,omitempty"`
}

// OrderbookConf holds configuration specific to order book data.
type OrderbookConf struct {
	Snapshots *Configs `yaml:"snapshots,omitempty"`
	Delta     *Configs `yaml:"delta,omitempty"`
}

// Configs holds the common configuration parameters for a data source.
type Configs struct {
	Enabled     bool     `yaml:"enabled"`
	Connection  string   `yaml:"connection"`
	URL         string   `yaml:"url"`
	Limit       int      `yaml:"limit,omitempty"`
	IntervalMS  int      `yaml:"interval_ms,omitempty"`
	IntervalMin int      `yaml:"interval_minutes,omitempty"`
	Symbols     []string `yaml:"symbols,omitempty"`
}

// LoadConfig reads a YAML configuration file from the specified path and returns a SourceConf struct.
func LoadConfig(path string) (*SourceConf, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config file: %w", err)
	}

	var cfg SourceConf
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse YAML: %w", err)
	}
	return &cfg, nil
}

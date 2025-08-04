// Prepares HTTP client
// Ensures data/<symbol>/asks/YYYY-MM-DD_HH.parquet and data/<symbol>/bids/YYYY-MM-DD_HH.parquet output dirs exist
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type SpotOrderBookSnapshotStruct struct {
	URL     string   `yaml:"url"`
	Limit   int      `yaml:"limit"`
	Depth   int      `yaml:"depth"`
	Symbols []string `yaml:"symbols"`
}

var SpotOrderBookSnapshot map[string]map[string]interface{}

func LoadConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	raw := struct {
		Entries []struct {
			Exchange string   `yaml:"exchange"`
			URL      string   `yaml:"url"`
			Limit    int      `yaml:"limit"`
			Period   int      `yaml:"period"`
			Symbols  []string `yaml:"symbols"`
		} `yaml:"spot_orderbook_snapshot"`
	}{}

	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	SpotOrderBookSnapshot = make(map[string]map[string]interface{})
	for _, e := range raw.Entries {
		SpotOrderBookSnapshot[e.Exchange] = map[string]interface{}{
			"url":     e.URL,
			"limit":   e.Limit,
			"depth":   e.Depth,
			"symbols": e.Symbols,
		}
	}

	return nil
}

package config

import (
	"testing"
)

func TestLoadConfigAndPrint(t *testing.T) {
	configPath := "../../config.yml" // adjust path if needed

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error loading config: %v", err)
	}

	for exchangeName, exchange := range cfg.Source {
		t.Logf("Exchange: %s\n", exchangeName)

		if exchange.Spot != nil {
			t.Log("  Spot:")
			printMarketCategoryTest(t, exchange.Spot)
		}

		if exchange.Future != nil {
			t.Log("  Future:")
			printMarketCategoryTest(t, exchange.Future)
		}
	}
}

func printMarketCategoryTest(t *testing.T, mc *MarketConf) {
	if mc.Orderbook != nil {
		if mc.Orderbook.Snapshots != nil {
			t.Log("    Orderbook Snapshots:")
			printMarketDataTest(t, mc.Orderbook.Snapshots)
		}
		if mc.Orderbook.Delta != nil {
			t.Log("    Orderbook Delta:")
			printMarketDataTest(t, mc.Orderbook.Delta)
		}
	}
	if mc.Contracts != nil {
		t.Log("    Contracts:")
		printMarketDataTest(t, mc.Contracts)
	}
	if mc.Liquidation != nil {
		t.Log("    Liquidation:")
		printMarketDataTest(t, mc.Liquidation)
	}
	if mc.OpenInterest != nil {
		t.Log("    Open Interest:")
		printMarketDataTest(t, mc.OpenInterest)
	}
	if mc.FundingRate != nil {
		t.Log("    Funding Rate:")
		printMarketDataTest(t, mc.FundingRate)
	}
}

func printMarketDataTest(t *testing.T, md *Configs) {
	t.Logf("      Enabled: %v", md.Enabled)
	t.Logf("      Connection: %s", md.Connection)
	t.Logf("      URL: %s", md.URL)
	if md.Limit != 0 {
		t.Logf("      Limit: %d", md.Limit)
	}
	if md.IntervalMS != 0 {
		t.Logf("      Interval (ms): %d", md.IntervalMS)
	}
	if md.IntervalMin != 0 {
		t.Logf("      Interval (min): %d", md.IntervalMin)
	}
	if len(md.Symbols) > 0 {
		t.Logf("      Symbols: %v", md.Symbols)
	}
}

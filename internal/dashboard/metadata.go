package dashboard

import (
	"reflect"
	"sort"
	"strings"
	"time"

	"cryptoflow/config"
)

// ExchangeMetadata describes a supported exchange and the markets that are
// currently enabled for ingestion according to the application configuration.
type ExchangeMetadata struct {
	Name        string           `json:"name"`
	DisplayName string           `json:"display_name"`
	Markets     []MarketMetadata `json:"markets"`
}

// MarketMetadata summarises a single exchange market (for example, futures
// orderbook snapshots). The Channels field lists the channel size metric names
// associated with the market so the dashboard can render focused insights.
type MarketMetadata struct {
	Key         string                 `json:"key"`
	DisplayName string                 `json:"display_name"`
	Description string                 `json:"description,omitempty"`
	Channels    []string               `json:"channels"`
	Symbols     []string               `json:"symbols"`
	Config      map[string]interface{} `json:"config,omitempty"`
}

const (
	marketOrderbookSnapshot = "future-orderbook-snapshot"
	marketOrderbookDelta    = "future-orderbook-delta"
	marketLiquidation       = "liquidation"
	marketOpenInterest      = "future-openinterest"
	marketPremiumIndex      = "future-premium-index"
)

var marketDisplayName = map[string]string{
	marketOrderbookSnapshot: "Futures Orderbook Snapshots",
	marketOrderbookDelta:    "Futures Orderbook Delta",
	marketLiquidation:       "Liquidations",
	marketOpenInterest:      "Open Interest",
	marketPremiumIndex:      "Premium Index",
}

var marketDescriptions = map[string]string{
	marketOrderbookSnapshot: "REST snapshots used for cold-starting the futures orderbook pipeline.",
	marketOrderbookDelta:    "Streaming deltas applied on top of snapshots to maintain depth.",
	marketLiquidation:       "Realtime liquidation feed for leveraged contracts.",
	marketOpenInterest:      "Open interest snapshots for futures instruments.",
	marketPremiumIndex:      "Fair funding premium index stream.",
}

var marketChannelMap = map[string][]string{
	marketOrderbookSnapshot: {"fobs_raw_buffer_length", "fobs_norm_buffer_length"},
	marketOrderbookDelta:    {"fobd_raw_buffer_length", "fobd_norm_buffer_length"},
	marketLiquidation:       {"liq_raw_buffer_length", "liq_norm_buffer_length"},
	marketOpenInterest:      {"foi_raw_buffer_length", "foi_norm_buffer_length"},
	marketPremiumIndex:      {"pi_raw_buffer_length", "pi_norm_buffer_length"},
}

func buildExchangeMetadata(cfg *config.Config) []ExchangeMetadata {
	if cfg == nil {
		return nil
	}

	var exchanges []ExchangeMetadata

	if markets := buildBinanceMarkets(cfg.Source.Binance); len(markets) > 0 {
		exchanges = append(exchanges, ExchangeMetadata{
			Name:        "binance",
			DisplayName: "Binance",
			Markets:     markets,
		})
	}

	if markets := buildBybitMarkets(cfg.Source.Bybit); len(markets) > 0 {
		exchanges = append(exchanges, ExchangeMetadata{
			Name:        "bybit",
			DisplayName: "Bybit",
			Markets:     markets,
		})
	}

	if markets := buildKucoinMarkets(cfg.Source.Kucoin); len(markets) > 0 {
		exchanges = append(exchanges, ExchangeMetadata{
			Name:        "kucoin",
			DisplayName: "KuCoin",
			Markets:     markets,
		})
	}

	if markets := buildOkxMarkets(cfg.Source.Okx); len(markets) > 0 {
		exchanges = append(exchanges, ExchangeMetadata{
			Name:        "okx",
			DisplayName: "OKX",
			Markets:     markets,
		})
	}

	return exchanges
}

func buildBinanceMarkets(src config.BinanceSourceConfig) []MarketMetadata {
	var markets []MarketMetadata
	if src.Future.Orderbook.Snapshots.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookSnapshot,
			src.Future.Orderbook.Snapshots.Symbols,
			structToConfigMap(src.Future.Orderbook.Snapshots, "symbols"),
		))
	}
	if src.Future.Orderbook.Delta.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookDelta,
			src.Future.Orderbook.Delta.Symbols,
			structToConfigMap(src.Future.Orderbook.Delta, "symbols"),
		))
	}
	if src.Future.Liquidation.Enabled {
		markets = append(markets, newMarketMetadata(
			marketLiquidation,
			src.Future.Liquidation.Symbols,
			structToConfigMap(src.Future.Liquidation, "symbols"),
		))
	}
	if src.Future.OpenInterest.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOpenInterest,
			src.Future.OpenInterest.Symbols,
			structToConfigMap(src.Future.OpenInterest, "symbols"),
		))
	}
	if src.Future.PremiumIndex.Enabled {
		markets = append(markets, newMarketMetadata(
			marketPremiumIndex,
			src.Future.PremiumIndex.Symbols,
			structToConfigMap(src.Future.PremiumIndex, "symbols"),
		))
	}
	return markets
}

func buildBybitMarkets(src config.BybitSourceConfig) []MarketMetadata {
	var markets []MarketMetadata
	if src.Future.Orderbook.Snapshots.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookSnapshot,
			src.Future.Orderbook.Snapshots.Symbols,
			structToConfigMap(src.Future.Orderbook.Snapshots, "symbols"),
		))
	}
	if src.Future.Orderbook.Delta.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookDelta,
			src.Future.Orderbook.Delta.Symbols,
			structToConfigMap(src.Future.Orderbook.Delta, "symbols"),
		))
	}
	if src.Future.Liquidation.Enabled {
		markets = append(markets, newMarketMetadata(
			marketLiquidation,
			src.Future.Liquidation.Symbols,
			structToConfigMap(src.Future.Liquidation, "symbols"),
		))
	}
	if src.Future.OpenInterest.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOpenInterest,
			src.Future.OpenInterest.Symbols,
			structToConfigMap(src.Future.OpenInterest, "symbols"),
		))
	}
	if src.Future.PremiumIndex.Enabled {
		markets = append(markets, newMarketMetadata(
			marketPremiumIndex,
			src.Future.PremiumIndex.Symbols,
			structToConfigMap(src.Future.PremiumIndex, "symbols"),
		))
	}
	return markets
}

func buildKucoinMarkets(src config.KucoinSourceConfig) []MarketMetadata {
	var markets []MarketMetadata
	if src.Future.Orderbook.Snapshots.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookSnapshot,
			src.Future.Orderbook.Snapshots.Symbols,
			structToConfigMap(src.Future.Orderbook.Snapshots, "symbols"),
		))
	}
	if src.Future.Orderbook.Delta.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookDelta,
			src.Future.Orderbook.Delta.Symbols,
			structToConfigMap(src.Future.Orderbook.Delta, "symbols"),
		))
	}
	if src.Future.Liquidation.Enabled {
		markets = append(markets, newMarketMetadata(
			marketLiquidation,
			src.Future.Liquidation.Symbols,
			structToConfigMap(src.Future.Liquidation, "symbols"),
		))
	}
	if src.Future.OpenInterest.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOpenInterest,
			src.Future.OpenInterest.Symbols,
			structToConfigMap(src.Future.OpenInterest, "symbols"),
		))
	}
	if src.Future.PremiumIndex.Enabled {
		markets = append(markets, newMarketMetadata(
			marketPremiumIndex,
			src.Future.PremiumIndex.Symbols,
			structToConfigMap(src.Future.PremiumIndex, "symbols"),
		))
	}
	return markets
}

func buildOkxMarkets(src config.OkxSourceConfig) []MarketMetadata {
	var markets []MarketMetadata
	if src.Future.Orderbook.Snapshots.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookSnapshot,
			src.Future.Orderbook.Snapshots.Symbols,
			structToConfigMap(src.Future.Orderbook.Snapshots, "symbols"),
		))
	}
	if src.Future.Orderbook.Delta.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOrderbookDelta,
			src.Future.Orderbook.Delta.Symbols,
			structToConfigMap(src.Future.Orderbook.Delta, "symbols"),
		))
	}
	if src.Future.Liquidation.Enabled {
		markets = append(markets, newMarketMetadata(
			marketLiquidation,
			src.Future.Liquidation.Symbols,
			structToConfigMap(src.Future.Liquidation, "symbols"),
		))
	}
	if src.Future.OpenInterest.Enabled {
		markets = append(markets, newMarketMetadata(
			marketOpenInterest,
			src.Future.OpenInterest.Symbols,
			structToConfigMap(src.Future.OpenInterest, "symbols"),
		))
	}
	if src.Future.PremiumIndex.Enabled {
		markets = append(markets, newMarketMetadata(
			marketPremiumIndex,
			src.Future.PremiumIndex.Symbols,
			structToConfigMap(src.Future.PremiumIndex, "symbols"),
		))
	}
	return markets
}

func newMarketMetadata(key string, symbols []string, cfg map[string]interface{}) MarketMetadata {
	meta := MarketMetadata{
		Key:         key,
		DisplayName: marketDisplayName[key],
		Description: marketDescriptions[key],
		Channels:    marketChannelMap[key],
		Symbols:     normalizeSymbols(symbols),
		Config:      cfg,
	}
	return meta
}

func normalizeSymbols(symbols []string) []string {
	if len(symbols) == 0 {
		return nil
	}
	uniq := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		s := strings.ToUpper(strings.TrimSpace(symbol))
		if s == "" {
			continue
		}
		uniq[s] = struct{}{}
	}
	if len(uniq) == 0 {
		return nil
	}
	out := make([]string, 0, len(uniq))
	for symbol := range uniq {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

func structToConfigMap(input interface{}, skipFields ...string) map[string]interface{} {
	val := reflect.ValueOf(input)
	if !val.IsValid() {
		return nil
	}
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	skip := make(map[string]struct{}, len(skipFields))
	for _, field := range skipFields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		skip[field] = struct{}{}
	}

	typ := val.Type()
	out := make(map[string]interface{})
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		key := yamlKey(field)
		if key == "" {
			continue
		}
		if _, omit := skip[key]; omit {
			continue
		}
		fieldVal := val.Field(i)
		if !fieldVal.CanInterface() || !fieldVal.IsValid() {
			continue
		}
		if fieldVal.Kind() == reflect.Slice || fieldVal.Kind() == reflect.Array {
			// Slices (e.g. symbol lists) are handled separately.
			continue
		}
		if fieldVal.Kind() == reflect.Struct {
			if _, ok := fieldVal.Interface().(time.Time); !ok {
				continue
			}
		}
		var value interface{}
		switch fieldVal.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if fieldVal.Type() == reflect.TypeOf(time.Duration(0)) {
				d := fieldVal.Interface().(time.Duration)
				if d == 0 {
					continue
				}
				value = d.String()
			} else {
				value = fieldVal.Interface()
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value = fieldVal.Interface()
		case reflect.Float32, reflect.Float64:
			value = fieldVal.Interface()
		case reflect.Bool:
			value = fieldVal.Interface()
		case reflect.String:
			if fieldVal.Len() == 0 {
				continue
			}
			value = fieldVal.Interface()
		default:
			value = fieldVal.Interface()
		}
		if isZeroValue(fieldVal) {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func yamlKey(field reflect.StructField) string {
	tag := field.Tag.Get("yaml")
	if tag != "" && tag != "-" {
		parts := strings.Split(tag, ",")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	return camelToSnake(field.Name)
}

func camelToSnake(input string) string {
	if input == "" {
		return ""
	}
	var builder strings.Builder
	for i, r := range input {
		if r >= 'A' && r <= 'Z' {
			if i != 0 {
				builder.WriteByte('_')
			}
			builder.WriteRune(r + 32)
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func isZeroValue(v reflect.Value) bool {
	return !v.IsValid() || v.IsZero()
}

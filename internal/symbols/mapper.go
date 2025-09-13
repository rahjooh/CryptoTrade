package symbols

import "strings"

// ToBinance converts various exchange-specific symbol formats to Binance style.
// It ensures symbols are uppercase without separators and uses BTC instead of XBT.
// Currently supported exchanges: binance, bybit, kucoin, coinbase, kraken, okx.
func ToBinance(exchange, sym string) string {
	switch strings.ToLower(exchange) {
	case "binance":
		switch sym {
		case "1000BONKUSDT":
			sym = "BONKUSDT"
		case "1000PEPEUSDT":
			sym = "PEPEUSDT"
		case "1000SHIBUSDT":
			sym = "SHIBUSDT"
		}
	case "bybit":
		switch sym {
		case "1000BONKUSDT":
			sym = "BONKUSDT"
		case "1000PEPEUSDT":
			sym = "PEPEUSDT"
		case "SHIB1000USDT":
			sym = "SHIBUSDT"
		}
	case "coinbase":
		sym = strings.ReplaceAll(sym, "-", "")
	case "kraken":
		sym = strings.ReplaceAll(sym, "/", "")
		sym = strings.ReplaceAll(sym, "-", "")
	case "kucoin":
		sym = strings.ReplaceAll(sym, "-", "")
		sym = strings.TrimSuffix(sym, "M")
		if strings.HasPrefix(sym, "XBT") {
			sym = "BTC" + sym[3:]
		}
	case "okx":
		sym = strings.TrimSuffix(sym, "-SWAP")
		sym = strings.ReplaceAll(sym, "-", "")
	default:
		// others already use the desired format
	}
	return sym
}

package symbols

import "strings"

// ToBinance converts various exchange-specific symbol formats to Binance style.
// It ensures symbols are uppercase without separators and uses BTC instead of XBT.
// Currently supported exchanges: binance, bybit, kucoin, coinbase, kraken.
func ToBinance(exchange, sym string) string {
	switch strings.ToLower(exchange) {
	case "kucoin":
		return NormalizeKucoinSymbol(sym)
	case "coinbase":
		sym = strings.ReplaceAll(sym, "-", "")
	case "kraken":
		sym = strings.ReplaceAll(sym, "/", "")
		sym = strings.ReplaceAll(sym, "-", "")
	default:
		// binance, bybit and others already use the desired format
	}

	sym = strings.ToUpper(sym)
	if strings.HasPrefix(sym, "XBT") {
		sym = "BTC" + sym[3:]
	}
	return sym
}

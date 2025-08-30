package symbols

import "strings"

// ToBinance converts various exchange-specific symbol formats to Binance style.
// It ensures symbols are uppercase without separators and uses BTC instead of XBT.
// Currently supported exchanges: binance, bybit, kucoin, coinbase, kraken.
func ToBinance(exchange, sym string) string {
	switch strings.ToLower(exchange) {
	case "kucoin":
		// remove dashes
		sym = strings.ReplaceAll(sym, "-", "")
		// trim trailing 'M' denoting futures
		sym = strings.TrimSuffix(sym, "M")
		// map XBT to BTC for compatibility
		if strings.HasPrefix(sym, "XBT") {
			sym = "BTC" + sym[3:]
		}
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

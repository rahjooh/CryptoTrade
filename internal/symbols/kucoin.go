package symbols

import "strings"

// NormalizeKucoinSymbol converts KuCoin futures symbols to a common format.
// Examples:
//
//	XBTUSDTM -> BTCUSDT
//	ETHUSDTM -> ETHUSDT
//
// The function removes dashes, trims trailing 'M', and maps XBT->BTC.
func NormalizeKucoinSymbol(sym string) string {
	// remove dashes
	sym = strings.ReplaceAll(sym, "-", "")
	// trim trailing 'M' denoting futures
	sym = strings.TrimSuffix(sym, "M")
	// map XBT to BTC for compatibility
	if strings.HasPrefix(sym, "XBT") {
		sym = "BTC" + sym[3:]
	}
	return sym
}

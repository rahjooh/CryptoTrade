package symbols

import "testing"

func TestToBinance(t *testing.T) {
	tests := []struct {
		exchange string
		in       string
		want     string
	}{
		{"kucoin", "XBT-USDTM", "BTCUSDT"},
		{"coinbase", "btc-usd", "BTCUSD"},
		{"kraken", "xbt/usd", "BTCUSD"},
		{"binance", "ethusdt", "ETHUSDT"},
		{"binance", "1000bonkusdt", "BONKUSDT"},
		{"binance", "1000pepeusdt", "PEPEUSDT"},
		{"binance", "1000shibusdt", "SHIBUSDT"},
	}
	for _, tt := range tests {
		if got := ToBinance(tt.exchange, tt.in); got != tt.want {
			t.Errorf("ToBinance(%s,%s)=%s want %s", tt.exchange, tt.in, got, tt.want)
		}
	}
}

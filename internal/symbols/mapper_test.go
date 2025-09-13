package symbols

import "testing"

func TestToBinance(t *testing.T) {
	tests := []struct {
		exchange string
		in       string
		want     string
	}{
		{"kucoin", "XBT-USDTM", "BTCUSDT"},
		{"coinbase", "BTC-USD", "BTCUSD"},
		{"kraken", "BTC/USD", "BTCUSD"},
		{"binance", "ETHUSDT", "ETHUSDT"},
		{"binance", "1000BONKUSDT", "BONKUSDT"},
		{"binance", "1000PEPEUSDT", "PEPEUSDT"},
		{"binance", "1000SHIBUSDT", "SHIBUSDT"},
		{"bybit", "SHIB1000USDT", "SHIBUSDT"},
		{"bybit", "1000BONKUSDT", "BONKUSDT"},
		{"bybit", "1000PEPEUSDT", "PEPEUSDT"},
	}
	for _, tt := range tests {
		if got := ToBinance(tt.exchange, tt.in); got != tt.want {
			t.Errorf("ToBinance(%s,%s)=%s want %s", tt.exchange, tt.in, got, tt.want)
		}
	}
}

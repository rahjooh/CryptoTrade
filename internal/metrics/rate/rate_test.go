package rate

import (
	"net/http"
	"testing"

	"cryptoflow/logger"
)

func TestReportKucoinSnapshotWeight(t *testing.T) {
	log := logger.GetLogger()
	header := http.Header{}
	header.Set("gw-ratelimit-remaining", "1990")
	header.Set("gw-ratelimit-reset", "1000")
	ReportKucoinSnapshotWeight(log, header, 0, 2000, "")
}

func TestReportKucoinWSWeight(t *testing.T) {
	log := logger.GetLogger()
	tracker := NewKucoinWSWeightTracker()
	tracker.RegisterOutgoing(50)
	tracker.RegisterConnectionAttempt()
	ReportKucoinWSWeight(log, tracker, "")
}

func TestReportRateLimitExceeded(t *testing.T) {
	log := logger.GetLogger()
	ReportRateLimitExceeded(log, "binance", "BTCUSDT", "127.0.0.1", "fobs")
}

func TestReportIPBan(t *testing.T) {
	log := logger.GetLogger()
	ReportIPBan(log, "binance", "BTCUSDT", "127.0.0.1", "fobs")
}

func TestDetectLimit(t *testing.T) {
	cases := []struct {
		exchange string
		msg      string
		rate     bool
		ban      bool
	}{
		{"binance", "Too many requests", true, false},
		{"okx", "IP has been blocked for 60 seconds", false, true},
		{"kucoin", "429 Too Many Requests", true, false},
		{"bybit", "IP rate limit reached", false, true},
		{"unknown", "hello world", false, false},
	}
	for _, c := range cases {
		rl, ban := detectLimit(c.exchange, c.msg)
		if rl != c.rate {
			t.Errorf("exchange %s: expected rateLimit %v got %v", c.exchange, c.rate, rl)
		}
		if ban != c.ban {
			t.Errorf("exchange %s: expected ipBan %v got %v", c.exchange, c.ban, ban)
		}
	}
}

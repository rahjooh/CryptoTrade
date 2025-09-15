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
	header.Set("gw-ratelimit-limit", "2000")
	ReportKucoinSnapshotWeight(log, header, "")
}

func TestReportKucoinWSWeight(t *testing.T) {
	log := logger.GetLogger()
	tracker := NewKucoinWSWeightTracker()
	tracker.RegisterOutgoing(50)
	tracker.RegisterConnectionAttempt()
	ReportKucoinWSWeight(log, tracker, "")
}

func TestReportBybitSnapshotWeight(t *testing.T) {
	log := logger.GetLogger()

	t.Run("legacy_headers", func(t *testing.T) {
		header := http.Header{}
		header.Set("X-Bapi-Limit", "120")
		header.Set("X-Bapi-Limit-Status", "100")
		header.Set("X-Bapi-Limit-Reset-Timestamp", "1000")
		ReportBybitSnapshotWeight(log, header, "")
	})

	t.Run("rate_limit_headers", func(t *testing.T) {
		header := http.Header{}
		header.Set("X-RateLimit-Limit", "120")
		header.Set("X-RateLimit-Remaining", "100")
		header.Set("X-RateLimit-Reset", "1000")
		ReportBybitSnapshotWeight(log, header, "")
	})

	t.Run("status_pair", func(t *testing.T) {
		header := http.Header{}
		header.Set("X-Bapi-Limit-Status", "40/120")
		ReportBybitSnapshotWeight(log, header, "")
	})
}

func TestReportOkxSnapshotWeight(t *testing.T) {
	log := logger.GetLogger()
	header := http.Header{}
	header.Set("Rate-Limit-Limit", "60;w=60")
	header.Set("Rate-Limit-Remaining", "59;w=60")
	ReportOkxSnapshotWeight(log, header, "")
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

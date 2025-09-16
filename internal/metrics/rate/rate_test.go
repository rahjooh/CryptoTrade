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

func TestComputeBybitUsed(t *testing.T) {
	cases := []struct {
		name   string
		limit  string
		status string
		used   string
		ip     string
		want   int64
	}{
		{
			name:   "remaining_only",
			limit:  "120",
			status: "100",
			want:   20,
		},
		{
			name:   "used_limit_pair",
			status: "40/120",
			want:   40,
		},
		{
			name:   "ip_scoped_json",
			limit:  "{\"per_ip\":{\"1.1.1.1\":{\"limit\":120},\"2.2.2.2\":{\"limit\":90}}}",
			status: "{\"per_ip\":{\"1.1.1.1\":{\"limit\":120,\"remaining\":90},\"2.2.2.2\":{\"limit\":90,\"remaining\":80}}}",
			ip:     "1.1.1.1",
			want:   30,
		},
		{
			name:  "used_header_json",
			limit: "120",
			used:  "{\"1.1.1.1\":55,\"2.2.2.2\":10}",
			ip:    "1.1.1.1",
			want:  55,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeBybitUsed(tc.limit, tc.status, tc.used, tc.ip)
			if got != tc.want {
				t.Fatalf("expected %d got %d", tc.want, got)
			}
		})
	}
}
func TestComputeOkxSnapshotUsedWeight(t *testing.T) {
	t.Run("single_window", func(t *testing.T) {
		header := http.Header{}
		header.Set("Rate-Limit-Limit", "60;w=60")
		header.Set("Rate-Limit-Remaining", "59;w=60")
		got := computeOkxSnapshotUsedWeight(header)
		if got != 1 {
			t.Fatalf("expected used weight 1, got %d", got)
		}
	})

	t.Run("multiple_windows", func(t *testing.T) {
		header := http.Header{}
		header.Add("Rate-Limit-Limit", "2;window=2s;type=public")
		header.Add("Rate-Limit-Limit", "240;window=60s;type=public")
		header.Add("Rate-Limit-Remaining", "2;window=2s;type=public")
		header.Add("Rate-Limit-Remaining", "180;window=60s;type=public")
		header.Add("Rate-Limit-Used", "0;window=2s;type=public")
		header.Add("Rate-Limit-Used", "60;window=60s;type=public")
		got := computeOkxSnapshotUsedWeight(header)
		if got != 60 {
			t.Fatalf("expected used weight 60, got %d", got)
		}
	})

	t.Run("fallback_difference", func(t *testing.T) {
		header := http.Header{}
		header.Set("X-RateLimit-Limit", "600;w=60")
		header.Set("X-RateLimit-Remaining", "450;w=60")
		got := computeOkxSnapshotUsedWeight(header)
		if got != 150 {
			t.Fatalf("expected used weight 150, got %d", got)
		}
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

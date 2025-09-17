package okxmetrics

import (
	"net/http"
	"testing"

	"cryptoflow/logger"
)

func TestExtractRateLimit(t *testing.T) {
	header := http.Header{}
	header.Set("Rate-Limit-Limit", "12")
	header.Set("Rate-Limit-Remaining", "4")
	header.Set("Rate-Limit-Reset", "1737043200")
	header.Set("Rate-Limit-Interval", "2s")

	rl := ExtractRateLimit(header)
	if rl.Limit != 12 {
		t.Fatalf("expected limit 12 got %v", rl.Limit)
	}
	if rl.Remaining != 4 {
		t.Fatalf("expected remaining 4 got %v", rl.Remaining)
	}
	if rl.WindowSecond != 2 {
		t.Fatalf("expected window 2 got %v", rl.WindowSecond)
	}
	if rl.ResetUnixMs != 1737043200*1000 {
		t.Fatalf("unexpected reset %v", rl.ResetUnixMs)
	}
}

func TestEstimateSnapshotWeightPerMinute(t *testing.T) {
	expected := (1000.0 / 1000.0) * 2 * 60 * SnapshotWeightPerRequest
	got := EstimateSnapshotWeightPerMinute(2, 1000, SnapshotWeightPerRequest)
	if got != expected {
		t.Fatalf("expected %v got %v", expected, got)
	}

	if EstimateSnapshotWeightPerMinute(0, 1000, SnapshotWeightPerRequest) != 0 {
		t.Fatalf("expected zero weight for zero symbols")
	}
}

func TestEstimateWebsocketConnectionPressure(t *testing.T) {
	expected := WebsocketConnLimitPerSec * 60
	if got := EstimateWebsocketConnectionPressure(1); got != expected {
		t.Fatalf("expected %v got %v", expected, got)
	}
	if EstimateWebsocketConnectionPressure(0) != 0 {
		t.Fatalf("expected zero for zero connections")
	}
}

func TestReportUsage(t *testing.T) {
	log := logger.GetLogger()
	rl := RateLimitSnapshot{Limit: 10, Remaining: 5, ResetUnixMs: 12345, WindowSecond: 2}
	if !ReportUsage(log, "okx_reader", "BTC-USDT-SWAP", "swap-orderbook-snapshot", "127.0.0.1", rl, SnapshotWeightPerRequest, 50) {
		t.Fatalf("expected metrics to be emitted")
	}
}

package kucoinmetrics

import (
	"testing"
	"time"

	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

func TestEstimateWeightPerMinute(t *testing.T) {
	got := EstimateWeightPerMinute(2, 500, FullSnapshotWeight)
	expected := (1000.0 / 500.0) * 2 * 60 * FullSnapshotWeight
	if got != expected {
		t.Fatalf("expected %v got %v", expected, got)
	}

	if EstimateWeightPerMinute(0, 500, FullSnapshotWeight) != 0 {
		t.Fatalf("expected zero weight for zero symbols")
	}

	if EstimateWeightPerMinute(2, 0, FullSnapshotWeight) != 0 {
		t.Fatalf("expected zero weight for zero interval")
	}
}

func TestReportUsage(t *testing.T) {
	log := logger.GetLogger()
	rl := RateLimitSnapshot{Limit: 400, Remaining: 200, Reset: 1737043200000}
	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	emitted := ReportUsage(log, "kucoin_reader", "XBTUSDTM", "future-orderbook-snapshot", "127.0.0.2", rl, FullSnapshotWeight, 10)
	if !emitted {
		t.Fatalf("expected metrics to be emitted")
	}

	select {
	case event := <-events:
		if len(event.Fields) != 1 {
			t.Fatalf("expected only ip field, got %v", event.Fields)
		}
		if ip, ok := event.Fields["ip"]; !ok || ip != "127.0.0.2" {
			t.Fatalf("expected ip field to be 127.0.0.2, got %v", event.Fields)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected metric event to be emitted")
	}
}

func TestReportUsage_NoLog(t *testing.T) {
	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	emitted := ReportUsage(nil, "kucoin_reader", "XBTUSDTM", "future-orderbook-snapshot", "", RateLimitSnapshot{}, FullSnapshotWeight, 0)
	if emitted {
		t.Fatalf("expected false when logger missing")
	}

	select {
	case <-events:
		t.Fatal("did not expect metric event when logger missing")
	case <-time.After(10 * time.Millisecond):
	}
}

package binancemetrics

import (
	"net/http"
	"testing"
	"time"

	"cryptoflow/config"
	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

func TestReportUsedWeight_Success(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-MBX-USED-WEIGHT-1M", "123.5")

	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	weight, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "127.0.0.1", 10)
	if !reported {
		t.Fatalf("expected metric to be reported")
	}
	if weight != 123.5 {
		t.Fatalf("unexpected weight: %v", weight)
	}

	select {
	case event := <-events:
		if len(event.Fields) != 1 {
			t.Fatalf("expected only ip field, got %v", event.Fields)
		}
		if ip, ok := event.Fields["ip"]; !ok || ip != "127.0.0.1" {
			t.Fatalf("expected ip field to be 127.0.0.1, got %v", event.Fields)
		}
	default:
		t.Fatal("expected metric event to be emitted")
	}
}

func TestReportUsedWeight_Invalid(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-MBX-USED-WEIGHT-1M", "not-a-number")

	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	if _, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "", 0); reported {
		t.Fatalf("expected no metric to be reported for invalid header")
	}

	select {
	case <-events:
		t.Fatal("did not expect metric emission for invalid header")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestReportUsedWeight_NoHeaders(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}

	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	if _, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "", 0); reported {
		t.Fatalf("expected no metric when headers missing")
	}

	select {
	case <-events:
		t.Fatal("did not expect metric emission when headers missing")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestEstimateWebsocketWeightPerMinute(t *testing.T) {
	weight := EstimateWebsocketWeightPerMinute(2, 100)
	expected := 2 * (1000.0 / 100.0) * 60.0
	if weight != expected {
		t.Fatalf("expected %v got %v", expected, weight)
	}

	if EstimateWebsocketWeightPerMinute(0, 100) != 0 {
		t.Fatalf("expected zero weight for zero symbols")
	}

	if EstimateWebsocketWeightPerMinute(2, 0) != 0 {
		t.Fatalf("expected zero weight for zero interval")
	}
}

func TestReportUsedWeight_Disabled(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-MBX-USED-WEIGHT-1M", "123.5")

	metrics.Configure(config.MetricsConfig{UsedWeight: false, ChannelSize: true})
	t.Cleanup(func() { metrics.Configure(config.MetricsConfig{UsedWeight: true, ChannelSize: true}) })

	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	if weight, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "127.0.0.1", 10); reported || weight != 0 {
		t.Fatalf("expected metrics to be disabled")
	}

	select {
	case <-events:
		t.Fatal("did not expect metric emission when feature disabled")
	case <-time.After(10 * time.Millisecond):
	}
}

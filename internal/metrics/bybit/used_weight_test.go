package bybitmetrics

import (
	"net/http"
	"testing"
	"time"

	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

func TestReportUsage_ParsesHeaders(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Bapi-Limit", "120")
	resp.Header.Set("X-Bapi-Limit-Status", "110")
	resp.Header.Set("X-Bapi-Limit-Reset-Timestamp", "1737043200000")

	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	limit, remaining, emitted := ReportUsage(log, resp, "bybit_reader", "BTCUSDT", "future-orderbook-snapshot", "10.0.0.1")
	if !emitted {
		t.Fatalf("expected metrics to be emitted")
	}
	if limit != 120 {
		t.Fatalf("expected limit 120, got %v", limit)
	}
	if remaining != 110 {
		t.Fatalf("expected remaining 110, got %v", remaining)
	}

	select {
	case event := <-events:
		if len(event.Fields) != 1 {
			t.Fatalf("expected only ip field, got %v", event.Fields)
		}
		if ip, ok := event.Fields["ip"]; !ok || ip != "10.0.0.1" {
			t.Fatalf("expected ip field to be 10.0.0.1, got %v", event.Fields)
		}
	default:
		t.Fatal("expected metric event to be emitted")
	}
}

func TestReportUsage_NoHeaders(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	if _, _, emitted := ReportUsage(log, resp, "bybit_reader", "BTCUSDT", "future-orderbook-snapshot", "10.0.0.1"); emitted {
		t.Fatalf("expected no metrics when headers missing")
	}

	select {
	case <-events:
		t.Fatal("did not expect metric emission when headers missing")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestReportUsage_InvalidNumbers(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Bapi-Limit", "abc")
	resp.Header.Set("X-Bapi-Limit-Status", "def")

	events := make(chan metrics.Metric, 1)
	id := metrics.RegisterMetricHandler(func(m metrics.Metric) { events <- m })
	t.Cleanup(func() { metrics.UnregisterMetricHandler(id) })

	_, _, emitted := ReportUsage(log, resp, "bybit_reader", "BTCUSDT", "future-orderbook-snapshot", "10.0.0.1")
	if !emitted {
		t.Fatalf("expected emit flag even when parsing fails to maintain logging consistency")
	}

	select {
	case <-events:
		t.Fatal("did not expect metric emission when parsing fails")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestParseLeadingFloat(t *testing.T) {
	cases := map[string]float64{
		"120":            120,
		"120-110":        120,
		"110-108-0":      110,
		"\t98 remaining": 98,
		"+72":            72,
	}

	for input, expected := range cases {
		got, ok := parseLeadingFloat(input)
		if !ok {
			t.Fatalf("expected parseLeadingFloat to succeed for %q", input)
		}
		if got != expected {
			t.Fatalf("expected %v for %q, got %v", expected, input, got)
		}
	}

	if _, ok := parseLeadingFloat("abc"); ok {
		t.Fatal("expected parseLeadingFloat to fail for non-numeric prefix")
	}
}

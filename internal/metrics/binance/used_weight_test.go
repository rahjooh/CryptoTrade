package binancemetrics

import (
	"net/http"
	"testing"

	"cryptoflow/logger"
)

func TestReportUsedWeight_Success(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-MBX-USED-WEIGHT-1M", "123.5")

	weight, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "127.0.0.1", 10)
	if !reported {
		t.Fatalf("expected metric to be reported")
	}
	if weight != 123.5 {
		t.Fatalf("unexpected weight: %v", weight)
	}
}

func TestReportUsedWeight_Invalid(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-MBX-USED-WEIGHT-1M", "not-a-number")

	if _, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "", 0); reported {
		t.Fatalf("expected no metric to be reported for invalid header")
	}
}

func TestReportUsedWeight_NoHeaders(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}

	if _, reported := ReportUsedWeight(log, resp, "binance_reader", "BTCUSDT", "snapshot", "", 0); reported {
		t.Fatalf("expected no metric when headers missing")
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

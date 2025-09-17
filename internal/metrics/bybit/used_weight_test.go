package bybitmetrics

import (
	"net/http"
	"testing"

	"cryptoflow/logger"
)

func TestReportUsage_ParsesHeaders(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Bapi-Limit", "120")
	resp.Header.Set("X-Bapi-Limit-Status", "110")
	resp.Header.Set("X-Bapi-Limit-Reset-Timestamp", "1737043200000")

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
}

func TestReportUsage_NoHeaders(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}

	if _, _, emitted := ReportUsage(log, resp, "bybit_reader", "BTCUSDT", "future-orderbook-snapshot", ""); emitted {
		t.Fatalf("expected no metrics when headers missing")
	}
}

func TestReportUsage_InvalidNumbers(t *testing.T) {
	log := logger.GetLogger()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Bapi-Limit", "abc")
	resp.Header.Set("X-Bapi-Limit-Status", "def")

	_, _, emitted := ReportUsage(log, resp, "bybit_reader", "BTCUSDT", "future-orderbook-snapshot", "")
	if !emitted {
		t.Fatalf("expected emit flag even when parsing fails to maintain logging consistency")
	}
}

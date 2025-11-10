package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cryptoflow/config"
	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

func TestMetricsEndpointEmitsStoredMetrics(t *testing.T) {
	log := logger.Logger()
	srv, err := NewServer(config.DashboardConfig{Enabled: true, RefreshInterval: time.Second, MetricsHistory: 10, LogHistory: 10}, log)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	t.Cleanup(srv.cleanup)

	metrics.EmitMetric(log, "component", "fobs_raw_buffer_length", 5, "gauge", logger.Fields{"capacity": 10})

	router, err := srv.buildRouter("app")
	if err != nil {
		t.Fatalf("buildRouter error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", res.Code)
	}
	if len(srv.metricStore.snapshot()) == 0 {
		t.Fatalf("metrics store empty")
	}
}

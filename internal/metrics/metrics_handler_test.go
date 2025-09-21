package metrics

import (
	"testing"
	"time"

	"cryptoflow/config"
	"cryptoflow/logger"
)

func resetMetricHandlers() {
	metricHandlersMu.Lock()
	metricHandlers = make(map[MetricHandlerID]MetricHandler)
	nextMetricHandlerID = 0
	metricHandlersMu.Unlock()
}

func TestRegisterMetricHandlerReturnsUniqueIDs(t *testing.T) {
	resetMetricHandlers()

	id := RegisterMetricHandler(func(Metric) {})
	if id == 0 {
		t.Fatalf("expected non-zero handler id")
	}

	second := RegisterMetricHandler(func(Metric) {})
	if second == 0 || second == id {
		t.Fatalf("expected unique handler id")
	}
}

func TestRegisterMetricHandlerNil(t *testing.T) {
	resetMetricHandlers()

	if id := RegisterMetricHandler(nil); id != 0 {
		t.Fatalf("expected zero id for nil handler, got %d", id)
	}
}

func TestEmitMetricDispatchesToHandlers(t *testing.T) {
	resetMetricHandlers()

	events := make(chan Metric, 1)
	id := RegisterMetricHandler(func(m Metric) {
		events <- m
	})
	t.Cleanup(func() {
		UnregisterMetricHandler(id)
	})

	fields := logger.Fields{"exchange": "binance", "unit": "count"}
	log := logger.Logger()

	EmitMetric(log, "used_weight", "request_count", 3, "gauge", fields)

	select {
	case event := <-events:
		if event.Component != "used_weight" {
			t.Fatalf("unexpected component: %s", event.Component)
		}
		if event.Name != "request_count" {
			t.Fatalf("unexpected metric name: %s", event.Name)
		}
		if event.Type != "gauge" {
			t.Fatalf("unexpected metric type: %s", event.Type)
		}
		if _, ok := fields["metric"]; ok {
			t.Fatalf("original fields mutated: %v", fields)
		}
		if _, ok := event.Fields["metric"]; ok {
			t.Fatalf("event fields should not contain metric key: %v", event.Fields)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("metric handler not invoked")
	}
}

func TestEmitMetricDefaultType(t *testing.T) {
	resetMetricHandlers()

	events := make(chan Metric, 1)
	id := RegisterMetricHandler(func(m Metric) {
		events <- m
	})
	t.Cleanup(func() {
		UnregisterMetricHandler(id)
	})

	EmitMetric(nil, "order_book", "updates", 7, "", logger.Fields{"unit": "count"})

	select {
	case event := <-events:
		if event.Type != "counter" {
			t.Fatalf("expected default metric type to be counter, got %s", event.Type)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("metric handler not invoked for default type")
	}
}

func TestEmitMetricWithoutName(t *testing.T) {
	resetMetricHandlers()

	events := make(chan Metric, 1)
	id := RegisterMetricHandler(func(m Metric) {
		events <- m
	})
	t.Cleanup(func() {
		UnregisterMetricHandler(id)
	})

	EmitMetric(nil, "component", "", 1, "counter", nil)

	select {
	case <-events:
		t.Fatal("handler should not receive metrics without a name")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEmitMetricDisabledFeature(t *testing.T) {
	resetMetricHandlers()

	Configure(config.MetricsConfig{UsedWeight: false, ChannelSize: false})
	t.Cleanup(func() { Configure(config.MetricsConfig{UsedWeight: true, ChannelSize: true}) })

	events := make(chan Metric, 1)
	id := RegisterMetricHandler(func(m Metric) {
		events <- m
	})
	t.Cleanup(func() {
		UnregisterMetricHandler(id)
	})

	EmitMetric(nil, "component", "used_weight", 1, "counter", nil)
	EmitMetric(nil, "component", "fobs_raw_buffer_length", 1, "gauge", nil)

	select {
	case <-events:
		t.Fatal("expected no metrics to be emitted when feature disabled")
	case <-time.After(20 * time.Millisecond):
	}
}

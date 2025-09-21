package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"cryptoflow/logger"
)

func TestPublishMetricDatumThrottlesToInterval(t *testing.T) {
	prevState := cwState.Load()
	cwState.Store(&cloudWatchState{client: &cloudwatch.Client{}})
	t.Cleanup(func() { cwState.Store(prevState) })

	resetMetricPublishTimes()
	t.Cleanup(resetMetricPublishTimes)

	originalInterval := cloudWatchPublishInterval
	cloudWatchPublishInterval = 50 * time.Millisecond
	t.Cleanup(func() { cloudWatchPublishInterval = originalInterval })

	baseTime := time.Now()
	timeNow = func() time.Time { return baseTime }
	t.Cleanup(func() { timeNow = time.Now })

	batches := make([][]cwtypes.MetricDatum, 0)
	publishMetricsFunc = func(ctx context.Context, state *cloudWatchState, data []cwtypes.MetricDatum) {
		copyData := make([]cwtypes.MetricDatum, len(data))
		copy(copyData, data)
		batches = append(batches, copyData)
	}
	t.Cleanup(func() { publishMetricsFunc = publishMetrics })

	metric := Metric{Component: "test", Name: "requests", Timestamp: baseTime, Fields: logger.Fields{"unit": "count"}}
	publishMetricDatum(metric, 1)

	timeNow = func() time.Time { return baseTime.Add(25 * time.Millisecond) }
	metric.Timestamp = baseTime.Add(25 * time.Millisecond)
	publishMetricDatum(metric, 2)

	if len(batches) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(batches))
	}

	if len(batches[0]) != 1 {
		t.Fatalf("expected single metric in publish, got %d", len(batches[0]))
	}

	datum := batches[0][0]
	if datum.MetricName == nil || *datum.MetricName != "requests" {
		t.Fatalf("unexpected metric name: %v", datum.MetricName)
	}
	if datum.Value == nil || *datum.Value != 1 {
		t.Fatalf("unexpected metric value: %v", datum.Value)
	}
}

func TestPublishMetricDatumAllowsAfterInterval(t *testing.T) {
	prevState := cwState.Load()
	cwState.Store(&cloudWatchState{client: &cloudwatch.Client{}})
	t.Cleanup(func() { cwState.Store(prevState) })

	resetMetricPublishTimes()
	t.Cleanup(resetMetricPublishTimes)

	originalInterval := cloudWatchPublishInterval
	cloudWatchPublishInterval = 50 * time.Millisecond
	t.Cleanup(func() { cloudWatchPublishInterval = originalInterval })

	baseTime := time.Now()
	timeNow = func() time.Time { return baseTime }
	t.Cleanup(func() { timeNow = time.Now })

	batches := make([][]cwtypes.MetricDatum, 0)
	publishMetricsFunc = func(ctx context.Context, state *cloudWatchState, data []cwtypes.MetricDatum) {
		copyData := make([]cwtypes.MetricDatum, len(data))
		copy(copyData, data)
		batches = append(batches, copyData)
	}
	t.Cleanup(func() { publishMetricsFunc = publishMetrics })

	metric := Metric{Component: "test", Name: "requests", Timestamp: baseTime, Fields: logger.Fields{"unit": "count"}}
	publishMetricDatum(metric, 1)

	timeNow = func() time.Time { return baseTime.Add(75 * time.Millisecond) }
	metric.Timestamp = baseTime.Add(75 * time.Millisecond)
	publishMetricDatum(metric, 2)

	if len(batches) != 2 {
		t.Fatalf("expected 2 publishes, got %d", len(batches))
	}

	second := batches[1]
	if len(second) != 1 {
		t.Fatalf("expected single metric in second publish, got %d", len(second))
	}

	datum := second[0]
	if datum.MetricName == nil || *datum.MetricName != "requests" {
		t.Fatalf("unexpected metric name: %v", datum.MetricName)
	}
	if datum.Value == nil || *datum.Value != 2 {
		t.Fatalf("unexpected metric value: %v", datum.Value)
	}
}

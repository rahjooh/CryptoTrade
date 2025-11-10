package dashboard

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"cryptoflow/internal/metrics"
)

func TestMetricStoreLimit(t *testing.T) {
	store := newMetricStore(2)
	for i := 0; i < 5; i++ {
		store.handle(metrics.Metric{Timestamp: time.Unix(int64(i), 0), Name: "metric", Value: i})
	}

	snapshot := store.snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 metrics in snapshot, got %d", len(snapshot))
	}

	if snapshot[0].Value != 3 || snapshot[1].Value != 4 {
		t.Fatalf("unexpected metrics retained: %#v", snapshot)
	}
}

func TestLogStoreCapturesEntries(t *testing.T) {
	store := newLogStore(3)
	entry := logrus.NewEntry(logrus.New())
	entry.Time = time.Unix(10, 0)
	entry.Level = logrus.WarnLevel
	entry.Message = "warning"
	entry.Data = logrus.Fields{"component": "test", "foo": "bar"}

	if err := store.Fire(entry); err != nil {
		t.Fatalf("store.Fire returned error: %v", err)
	}

	snapshot := store.snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(snapshot))
	}

	if snapshot[0].Component != "test" || snapshot[0].Fields["foo"] != "bar" {
		t.Fatalf("unexpected snapshot data: %#v", snapshot[0])
	}
}

func TestLogStoreRespectsLimitAndClose(t *testing.T) {
	store := newLogStore(2)
	for i := 0; i < 4; i++ {
		entry := logrus.NewEntry(logrus.New())
		entry.Message = "msg"
		entry.Level = logrus.InfoLevel
		entry.Data = logrus.Fields{"index": i}
		if err := store.Fire(entry); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	snapshot := store.snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 entries after pruning, got %d", len(snapshot))
	}

	store.close()
	entry := logrus.NewEntry(logrus.New())
	entry.Message = "ignored"
	if err := store.Fire(entry); err != nil {
		t.Fatalf("unexpected error after close: %v", err)
	}

	snapshot = store.snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("store accepted entries after close")
	}
}

package dashboard

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"cryptoflow/internal/metrics"
)

// metricStore retains a bounded collection of the most recent metrics that have been
// emitted by the application. It is safe for concurrent use.
type metricStore struct {
	mu    sync.RWMutex
	items []metrics.Metric
	limit int
}

func newMetricStore(limit int) *metricStore {
	if limit <= 0 {
		limit = 200
	}
	return &metricStore{limit: limit}
}

func (s *metricStore) handle(metric metrics.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(s.items, metric)
	if len(s.items) > s.limit {
		// keep the most recent entries only
		s.items = append([]metrics.Metric(nil), s.items[len(s.items)-s.limit:]...)
	}
}

func (s *metricStore) snapshot() []metrics.Metric {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]metrics.Metric, len(s.items))
	copy(out, s.items)
	return out
}

// logRecord is the serialisable representation of a captured log entry that is
// rendered in the dashboard UI.
type logRecord struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Component string                 `json:"component,omitempty"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// logStore retains the most recent logs that flow through the global logger. The
// store implements the logrus Hook interface so that it can be attached directly to
// the application's logger.
type logStore struct {
	mu      sync.RWMutex
	items   []logRecord
	limit   int
	enabled atomic.Bool
}

func newLogStore(limit int) *logStore {
	if limit <= 0 {
		limit = 200
	}
	ls := &logStore{limit: limit}
	ls.enabled.Store(true)
	return ls
}

func (s *logStore) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (s *logStore) Fire(entry *logrus.Entry) error {
	if !s.enabled.Load() {
		return nil
	}

	record := logRecord{
		Timestamp: entry.Time,
		Level:     entry.Level.String(),
		Message:   entry.Message,
	}

	if component, ok := entry.Data["component"].(string); ok {
		record.Component = component
	}

	if len(entry.Data) > 0 {
		record.Fields = make(map[string]interface{}, len(entry.Data))
		for k, v := range entry.Data {
			if k == "component" {
				continue
			}

			switch val := v.(type) {
			case error:
				record.Fields[k] = val.Error()
			case fmt.Stringer:
				record.Fields[k] = val.String()
			default:
				record.Fields[k] = val
			}
		}
	}

	s.mu.Lock()
	s.items = append(s.items, record)
	if len(s.items) > s.limit {
		s.items = append([]logRecord(nil), s.items[len(s.items)-s.limit:]...)
	}
	s.mu.Unlock()
	return nil
}

func (s *logStore) snapshot() []logRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]logRecord, len(s.items))
	copy(out, s.items)
	return out
}

func (s *logStore) close() {
	s.enabled.Store(false)
}

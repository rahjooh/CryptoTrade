package dashboard

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"cryptoflow/internal/metrics"
)

type dropEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Metric    string    `json:"metric"`
	Exchange  string    `json:"exchange,omitempty"`
	Market    string    `json:"market,omitempty"`
	Symbol    string    `json:"symbol,omitempty"`
	Stage     string    `json:"stage,omitempty"`
}

type dropLog struct {
	mu      sync.RWMutex
	entries []dropEntry
	limit   int
}

var dropMetricNames = map[string]struct{}{
	string(metrics.DropMetricSnapshotRaw):     {},
	string(metrics.DropMetricDeltaRaw):        {},
	string(metrics.DropMetricDeltaNorm):       {},
	string(metrics.DropMetricLiquidationRaw):  {},
	string(metrics.DropMetricOpenInterestRaw): {},
	string(metrics.DropMetricPremiumIndexRaw): {},
	string(metrics.DropMetricOther):           {},
}

func newDropLog(limit int) *dropLog {
	if limit <= 0 {
		limit = 500
	}
	return &dropLog{limit: limit}
}

func (d *dropLog) add(metric metrics.Metric) {
	if d == nil {
		return
	}
	if _, ok := dropMetricNames[metric.Name]; !ok {
		return
	}

	entry := dropEntry{
		Timestamp: metric.Timestamp,
		Metric:    metric.Name,
		Exchange:  fieldString(metric.Fields, "exchange"),
		Market:    fieldString(metric.Fields, "market"),
		Symbol:    fieldString(metric.Fields, "symbol"),
		Stage:     fieldString(metric.Fields, "stage"),
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	d.mu.Lock()
	d.entries = append(d.entries, entry)
	if len(d.entries) > d.limit {
		d.entries = append([]dropEntry(nil), d.entries[len(d.entries)-d.limit:]...)
	}
	d.mu.Unlock()
}

func (d *dropLog) snapshot() []dropEntry {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make([]dropEntry, len(d.entries))
	copy(out, d.entries)
	return out
}

func fieldString(fields map[string]interface{}, key string) string {
	if len(fields) == 0 {
		return ""
	}
	val, ok := fields[key]
	if !ok {
		return ""
	}
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

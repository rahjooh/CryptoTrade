package rate

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportBybitSnapshotWeight parses Bybit rate-limit headers and emits metrics.
// It expects headers X-Bapi-Limit, X-Bapi-Limit-Status and
// X-Bapi-Limit-Reset-Timestamp which provide the current limit, remaining
// requests and the reset timestamp in milliseconds respectively.
func ReportBybitSnapshotWeight(log *logger.Log, header http.Header) {
	limit, _ := strconv.ParseInt(header.Get("X-Bapi-Limit"), 10, 64)
	remaining, _ := strconv.ParseInt(header.Get("X-Bapi-Limit-Status"), 10, 64)
	resetTS, _ := strconv.ParseInt(header.Get("X-Bapi-Limit-Reset-Timestamp"), 10, 64)
	used := limit - remaining

	l := log.WithComponent("bybit_reader")
	l.LogMetric("bybit_reader", "used_weight", used, "gauge", logger.Fields{})
	l.LogMetric("bybit_reader", "remaining_weight", remaining, "gauge", logger.Fields{})
	l.LogMetric("bybit_reader", "limit", limit, "gauge", logger.Fields{})
	l.LogMetric("bybit_reader", "reset_timestamp", resetTS, "gauge", logger.Fields{})
	if limit > 0 {
		pct := float64(remaining) / float64(limit)
		l.LogMetric("bybit_reader", "remaining_ratio", pct, "gauge", logger.Fields{})
	}
}

// BybitWSWeightTracker tracks outgoing websocket messages and connection
// attempts for Bybit futures order book delta streams. While Bybit does not
// charge websocket market data against REST limits, tracking our own message
// and reconnect cadence helps avoid self-induced rate issues.
type BybitWSWeightTracker struct {
	mu       sync.Mutex
	window   time.Time
	msgs     int
	attempts int
}

// NewBybitWSWeightTracker creates a new tracker.
func NewBybitWSWeightTracker() *BybitWSWeightTracker {
	return &BybitWSWeightTracker{window: time.Now()}
}

// RegisterOutgoing records n outgoing client messages (subs/pings).
func (t *BybitWSWeightTracker) RegisterOutgoing(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.window) >= time.Second {
		t.msgs = 0
		t.window = now
	}
	t.msgs += n
}

// RegisterConnectionAttempt records a websocket handshake attempt.
func (t *BybitWSWeightTracker) RegisterConnectionAttempt() {
	t.mu.Lock()
	t.attempts++
	t.mu.Unlock()
}

// Stats returns the current message count within the one second window and the
// total connection attempts.
func (t *BybitWSWeightTracker) Stats() (msgs int, attempts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	msgs = t.msgs
	attempts = t.attempts
	return
}

// ReportBybitWSWeight emits websocket related weight metrics.
func ReportBybitWSWeight(log *logger.Log, t *BybitWSWeightTracker) {
	msgs, attempts := t.Stats()
	l := log.WithComponent("bybit_delta_reader")
	l.LogMetric("bybit_delta_reader", "outgoing_messages", int64(msgs), "gauge", logger.Fields{})
	l.LogMetric("bybit_delta_reader", "connection_attempts", int64(attempts), "counter", logger.Fields{})
}

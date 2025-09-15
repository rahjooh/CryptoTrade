package rate

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportBybitSnapshotWeight parses Bybit rate-limit headers and emits a
// `used_weight` metric for the given IP. It falls back between legacy
// X-Bapi-* headers and the newer X-RateLimit-* variants.
func ReportBybitSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	// Bybit has changed header names over time; try both the old X-Bapi-*
	// headers and the generic X-RateLimit-* variants. Missing headers are
	// treated as zero which previously resulted in metrics always reading
	// zero. Falling back ensures metrics are populated regardless of which
	// set is returned by the API.
	limitStr := header.Get("X-Bapi-Limit")
	if limitStr == "" {
		limitStr = header.Get("X-RateLimit-Limit")
	}

	remainingStr := header.Get("X-Bapi-Limit-Status")
	if remainingStr == "" {
		remainingStr = header.Get("X-RateLimit-Remaining")
	}

	limit, _ := strconv.ParseInt(limitStr, 10, 64)
	remaining, _ := strconv.ParseInt(remainingStr, 10, 64)
	used := limit - remaining
	if used < 0 {
		used = 0
	}

	l := log.WithComponent("bybit_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("bybit_reader", "used_weight", used, "gauge", fields)
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
func ReportBybitWSWeight(log *logger.Log, t *BybitWSWeightTracker, ip string) {
	msgs, attempts := t.Stats()
	l := log.WithComponent("bybit_delta_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("bybit_delta_reader", "outgoing_messages", int64(msgs), "gauge", fields)
	l.LogMetric("bybit_delta_reader", "connection_attempts", int64(attempts), "counter", fields)
}

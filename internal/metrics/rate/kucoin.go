package rate

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"cryptoflow/logger"
)

// kucoinSnapshotWeight returns the request weight for KuCoin snapshot endpoints.
// level 0 denotes a full L2 snapshot, while 20 and 100 correspond to partial
// depth20 and depth100 snapshots respectively.
func kucoinSnapshotWeight(level int) int64 {
	switch level {
	case 0:
		return 3
	case 20:
		return 5
	case 100:
		return 10
	default:
		return 0
	}
}

// ReportKucoinSnapshotWeight parses rate limit headers from KuCoin REST
// responses and emits metrics about weight usage and remaining quota.
func ReportKucoinSnapshotWeight(log *logger.Log, header http.Header, level int, limit int64, ip string) {
	remainingStr := header.Get("gw-ratelimit-remaining")
	remaining, _ := strconv.ParseInt(remainingStr, 10, 64)
	resetStr := header.Get("gw-ratelimit-reset")
	reset, _ := strconv.ParseInt(resetStr, 10, 64)

	if limit == 0 {
		limitStr := header.Get("gw-ratelimit-limit")
		limit, _ = strconv.ParseInt(limitStr, 10, 64)
	}

	endpointWeight := kucoinSnapshotWeight(level)

	l := log.WithComponent("kucoin_reader")
	fields := logger.Fields{"ip": ip}
	used := limit - remaining
	if used < 0 {
		used = 0
	}
	l.LogMetric("kucoin_reader", "used_weight", used, "gauge", fields)
	l.LogMetric("kucoin_reader", "remaining_weight", remaining, "gauge", fields)
	l.LogMetric("kucoin_reader", "limit", limit, "gauge", fields)
	if limit > 0 {
		pct := float64(remaining) / float64(limit)
		l.LogMetric("kucoin_reader", "remaining_ratio", pct, "gauge", fields)
	}
	l.LogMetric("kucoin_reader", "reset_ms", reset, "gauge", fields)
	fieldsWithLevel := logger.Fields{"level": level, "ip": ip}
	l.LogMetric("kucoin_reader", "endpoint_weight", endpointWeight, "gauge", fieldsWithLevel)
}

// KucoinWSWeightTracker tracks outgoing websocket messages and connection
// attempts for KuCoin futures depth streams.
type KucoinWSWeightTracker struct {
	mu            sync.Mutex
	msgWindow     time.Time
	msgs          int
	attemptWindow time.Time
	attempts      int
}

// NewKucoinWSWeightTracker creates a new tracker instance.
func NewKucoinWSWeightTracker() *KucoinWSWeightTracker {
	now := time.Now()
	return &KucoinWSWeightTracker{msgWindow: now, attemptWindow: now}
}

// RegisterOutgoing records n outgoing client messages such as subscriptions or pings.
func (t *KucoinWSWeightTracker) RegisterOutgoing(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.msgWindow) >= 10*time.Second {
		t.msgs = 0
		t.msgWindow = now
	}
	t.msgs += n
}

// RegisterConnectionAttempt records a websocket connection attempt.
func (t *KucoinWSWeightTracker) RegisterConnectionAttempt() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.attemptWindow) >= time.Minute {
		t.attempts = 0
		t.attemptWindow = now
	}
	t.attempts++
}

// Stats returns the number of messages sent in the current 10-second window and
// the number of connection attempts in the current minute.
func (t *KucoinWSWeightTracker) Stats() (msgs int, attempts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	msgs = t.msgs
	attempts = t.attempts
	return
}

// ReportKucoinWSWeight emits websocket-related weight metrics.
func ReportKucoinWSWeight(log *logger.Log, t *KucoinWSWeightTracker, ip string) {
	msgs, attempts := t.Stats()
	l := log.WithComponent("kucoin_delta_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("kucoin_delta_reader", "outgoing_messages_10s", int64(msgs), "gauge", fields)
	l.LogMetric("kucoin_delta_reader", "connection_attempts_min", int64(attempts), "gauge", fields)
}

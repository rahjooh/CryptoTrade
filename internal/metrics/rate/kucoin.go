package rate

import (
	"net/http"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportKucoinSnapshotWeight parses rate limit headers from KuCoin REST
// responses and emits a single `used_weight` metric for the given IP address.
func ReportKucoinSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	remainingVals := extractInts(header.Get("gw-ratelimit-remaining"))
	limitVals := extractInts(header.Get("gw-ratelimit-limit"))
	used := kucoinUsedWeight(limitVals, remainingVals)

	l := log.WithComponent("kucoin_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("kucoin_reader", "used_weight", used, "gauge", fields)
}

func kucoinUsedWeight(limitVals, remainingVals []int64) int64 {
	var limit, remaining int64
	if len(limitVals) > 0 && limitVals[0] > 0 {
		limit = limitVals[0]
	}
	if len(remainingVals) > 0 {
		remaining = remainingVals[0]
	} else {
		remaining = limit
	}
	if limit == 0 && remaining > 0 {
		limit = remaining
	}
	if remaining < 0 {
		remaining = limit
	}
	used := limit - remaining
	if used < 0 {
		used = 0
	}

	return used
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

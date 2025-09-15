package rate

import (
	"net/http"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportOkxSnapshotWeight parses rate-limit headers from OKX REST responses and
// emits a single `used_weight` metric for the originating IP. It accepts both
// standard and "X-" prefixed header variants.
func ReportOkxSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	limitStr := header.Get("Rate-Limit-Limit")
	if limitStr == "" {
		limitStr = header.Get("X-RateLimit-Limit")
	}
	remainingStr := header.Get("Rate-Limit-Remaining")
	if remainingStr == "" {
		remainingStr = header.Get("X-RateLimit-Remaining")
	}
	usedStr := header.Get("Rate-Limit-Used")
	if usedStr == "" {
		usedStr = header.Get("X-RateLimit-Used")
	}

	limitVals := extractInts(limitStr)
	remainingVals := extractInts(remainingStr)
	usedVals := extractInts(usedStr)

	var limit, remaining, used int64
	if len(limitVals) > 0 {
		limit = limitVals[0]
	}
	if len(remainingVals) > 0 {
		remaining = remainingVals[0]
	}
	if len(usedVals) > 0 {
		used = usedVals[0]
	} else if limit > 0 {
		used = limit - remaining
	}
	if used < 0 {
		used = 0
	}

	l := log.WithComponent("okx_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("okx_reader", "used_weight", used, "gauge", fields)
}

// OkxWSWeightTracker tracks websocket connection attempts and operations.
type OkxWSWeightTracker struct {
	mu          sync.Mutex
	secWindow   time.Time
	secAttempts int
	attempts    int
	hourWindow  time.Time
	opsHour     int
}

// NewOkxWSWeightTracker creates a new websocket tracker.
func NewOkxWSWeightTracker() *OkxWSWeightTracker {
	now := time.Now()
	return &OkxWSWeightTracker{
		secWindow:  now,
		hourWindow: now,
	}
}

// RegisterConnectionAttempt records a websocket connection attempt.
func (t *OkxWSWeightTracker) RegisterConnectionAttempt() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.secWindow) >= time.Second {
		t.secAttempts = 0
		t.secWindow = now
	}
	t.secAttempts++
	t.attempts++
}

// RegisterOp records n subscribe/unsubscribe operations.
func (t *OkxWSWeightTracker) RegisterOp(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.hourWindow) >= time.Hour {
		t.opsHour = 0
		t.hourWindow = now
	}
	t.opsHour += n
}

// Stats returns connection attempts in the current second, total attempts, and ops in the last hour.
func (t *OkxWSWeightTracker) Stats() (connSec int, totalAttempts int, opsHour int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.secWindow) >= time.Second {
		t.secAttempts = 0
		t.secWindow = now
	}
	if now.Sub(t.hourWindow) >= time.Hour {
		t.opsHour = 0
		t.hourWindow = now
	}
	connSec = t.secAttempts
	totalAttempts = t.attempts
	opsHour = t.opsHour
	return
}

// ReportOkxWSWeight emits websocket related weight metrics.
func ReportOkxWSWeight(log *logger.Log, t *OkxWSWeightTracker, ip string) {
	connSec, totalAttempts, opsHour := t.Stats()
	l := log.WithComponent("okx_delta_reader")
	remainingConn := 3 - connSec
	if remainingConn < 0 {
		remainingConn = 0
	}
	remainingOps := 480 - opsHour
	if remainingOps < 0 {
		remainingOps = 0
	}
	fields := logger.Fields{"ip": ip}
	l.LogMetric("okx_delta_reader", "connection_attempts_current_sec", int64(connSec), "gauge", fields)
	l.LogMetric("okx_delta_reader", "remaining_connections_current_sec", int64(remainingConn), "gauge", fields)
	l.LogMetric("okx_delta_reader", "connection_attempts_total", int64(totalAttempts), "counter", fields)
	l.LogMetric("okx_delta_reader", "ops_last_hour", int64(opsHour), "gauge", fields)
	l.LogMetric("okx_delta_reader", "remaining_ops_last_hour", int64(remainingOps), "gauge", fields)
}

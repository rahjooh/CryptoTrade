package rate

import (
	"sync"
	"time"

	"cryptoflow/logger"
)

// OkxRESTWeightTracker tracks REST request counts within a two second window.
type OkxRESTWeightTracker struct {
	mu       sync.Mutex
	window   time.Time
	count    int64
	limit    int64
	interval time.Duration
}

// NewOkxRESTWeightTracker creates a tracker for REST snapshot weight usage.
func NewOkxRESTWeightTracker(limit int64) *OkxRESTWeightTracker {
	return &OkxRESTWeightTracker{
		window:   time.Now(),
		limit:    limit,
		interval: 2 * time.Second,
	}
}

// RegisterRequest records one REST request.
func (t *OkxRESTWeightTracker) RegisterRequest() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.window) >= t.interval {
		t.count = 0
		t.window = now
	}
	t.count++
}

// Stats returns used and remaining weight within the current window.
func (t *OkxRESTWeightTracker) Stats() (used, remaining int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.window) >= t.interval {
		t.count = 0
		t.window = now
	}
	used = t.count
	remaining = t.limit - t.count
	if remaining < 0 {
		remaining = 0
	}
	return
}

// ReportOkxSnapshotWeight emits metrics for REST snapshot weight usage.
func ReportOkxSnapshotWeight(log *logger.Log, t *OkxRESTWeightTracker, ip string) {
	used, remaining := t.Stats()
	l := log.WithComponent("okx_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("okx_reader", "used_weight", used, "gauge", fields)
	l.LogMetric("okx_reader", "remaining_weight", remaining, "gauge", fields)
	l.LogMetric("okx_reader", "endpoint_weight", 1, "gauge", fields)
	near := int64(0)
	if t.limit > 0 && remaining*5 <= t.limit {
		near = 1
	}
	l.LogMetric("okx_reader", "near_limit", near, "gauge", fields)
	banned := int64(0)
	if remaining <= 0 {
		banned = 1
	}
	l.LogMetric("okx_reader", "banned", banned, "gauge", fields)
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

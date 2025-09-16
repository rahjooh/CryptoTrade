package rate

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportOkxSnapshotWeight parses rate-limit headers from OKX REST responses and
// emits a single `used_weight` metric for the originating IP. It accepts both
// standard and "X-" prefixed header variants.
func ReportOkxSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	used := computeOkxSnapshotUsedWeight(header)
	l := log.WithComponent("okx_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("okx_reader", "used_weight", used, "gauge", fields)
}

type okxRateEntry struct {
	value int64
	key   string
}

func computeOkxSnapshotUsedWeight(header http.Header) int64 {
	limitEntries := parseOkxRateEntries(header, "limit", "Rate-Limit-Limit", "X-RateLimit-Limit")
	remainingEntries := parseOkxRateEntries(header, "remaining", "Rate-Limit-Remaining", "X-RateLimit-Remaining")
	usedEntries := parseOkxRateEntries(header, "used", "Rate-Limit-Used", "X-RateLimit-Used")

	usedByWindow := buildOkxWindowValueMap(usedEntries)
	remainingByWindow := buildOkxWindowValueMap(remainingEntries)
	limitByWindow := buildOkxWindowValueMap(limitEntries)

	best := int64(0)
	for _, val := range usedByWindow {
		if val > best {
			best = val
		}
	}

	for window, limit := range limitByWindow {
		candidate := int64(0)
		if used, ok := usedByWindow[window]; ok {
			candidate = used
		}
		if limit > 0 {
			if remaining, ok := remainingByWindow[window]; ok {
				diff := limit - remaining
				if diff < 0 {
					diff = 0
				}
				if diff > candidate {
					candidate = diff
				}
			}
		}
		if candidate > best {
			best = candidate
		}
	}

	if best > 0 {
		return best
	}

	maxLen := len(limitEntries)
	if len(remainingEntries) > maxLen {
		maxLen = len(remainingEntries)
	}
	if len(usedEntries) > maxLen {
		maxLen = len(usedEntries)
	}

	for i := 0; i < maxLen; i++ {
		var (
			limitVal, remainingVal, usedVal    int64
			haveLimit, haveRemaining, haveUsed bool
		)
		if i < len(limitEntries) {
			limitVal = limitEntries[i].value
			haveLimit = true
		}
		if i < len(remainingEntries) {
			remainingVal = remainingEntries[i].value
			haveRemaining = true
		}
		if i < len(usedEntries) {
			usedVal = usedEntries[i].value
			haveUsed = true
		}

		candidate := int64(0)
		if haveUsed {
			candidate = usedVal
		}
		if (!haveUsed || candidate == 0) && haveLimit && haveRemaining {
			diff := limitVal - remainingVal
			if diff < 0 {
				diff = 0
			}
			if diff > candidate {
				candidate = diff
			}
		}
		if candidate > best {
			best = candidate
		}
	}

	if best < 0 {
		return 0
	}
	return best
}

func buildOkxWindowValueMap(entries []okxRateEntry) map[string]int64 {
	m := make(map[string]int64)
	for _, e := range entries {
		key := e.key
		if current, ok := m[key]; !ok || e.value > current {
			m[key] = e.value
		}
	}
	return m
}

func parseOkxRateEntries(header http.Header, target string, names ...string) []okxRateEntry {
	var entries []okxRateEntry
	target = strings.ToLower(target)
	for _, name := range names {
		for _, raw := range header.Values(name) {
			parts := strings.Split(raw, ",")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}

				tokens := strings.Split(part, ";")
				tagSet := make(map[string]struct{})
				var (
					value     int64
					haveValue bool
				)

				for _, token := range tokens {
					token = strings.TrimSpace(token)
					if token == "" {
						continue
					}
					lower := strings.ToLower(token)
					if strings.Contains(lower, "=") {
						kv := strings.SplitN(lower, "=", 2)
						key := strings.TrimSpace(kv[0])
						val := ""
						if len(kv) > 1 {
							val = strings.TrimSpace(kv[1])
						}
						if !haveValue && target != "" && strings.Contains(key, target) {
							if v, ok := parseOkxFirstInt(val); ok {
								value = v
								haveValue = true
							}
							continue
						}
						if !haveValue {
							switch {
							case strings.Contains(key, "limit"), strings.Contains(key, "remain"), strings.Contains(key, "used"), strings.Contains(key, "quota"), strings.Contains(key, "count"):
								if v, ok := parseOkxFirstInt(val); ok {
									value = v
									haveValue = true
								}
								continue
							}
						}
						if strings.Contains(key, "limit") || strings.Contains(key, "remain") || strings.Contains(key, "used") || strings.Contains(key, "quota") || strings.Contains(key, "count") {
							continue
						}
						tag := key
						if val != "" {
							tag = key + "=" + val
						}
						tagSet[tag] = struct{}{}
						continue
					}
					if !haveValue {
						if v, ok := parseOkxFirstInt(token); ok {
							value = v
							haveValue = true
							continue
						}
					}
					tagSet[lower] = struct{}{}
				}

				if !haveValue {
					continue
				}

				tags := make([]string, 0, len(tagSet))
				for tag := range tagSet {
					tags = append(tags, tag)
				}
				sort.Strings(tags)
				key := strings.Join(tags, "|")
				entries = append(entries, okxRateEntry{value: value, key: key})
			}
		}
	}
	return entries
}
func parseOkxFirstInt(s string) (int64, bool) {
	start := -1
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= '0' && ch <= '9' {
			if start == -1 {
				start = i
			}
		} else if start != -1 {
			val, err := strconv.ParseInt(s[start:i], 10, 64)
			if err != nil {
				return 0, false
			}
			return val, true
		}
	}
	if start != -1 {
		val, err := strconv.ParseInt(s[start:], 10, 64)
		if err != nil {
			return 0, false
		}
		return val, true
	}
	return 0, false
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

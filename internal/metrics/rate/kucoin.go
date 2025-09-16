package rate

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportKucoinSnapshotWeight parses rate limit headers from KuCoin REST
// responses and emits a single `used_weight` metric for the given IP address.
func ReportKucoinSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	used := computeKucoinUsedWeight(header)

	l := log.WithComponent("kucoin_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("kucoin_reader", "used_weight", used, "gauge", fields)
}

// computeKucoinUsedWeight extracts the best available request-weight usage from
// KuCoin REST headers. KuCoin occasionally omits the limit headers or reports
// negative sentinel values (e.g. "-1") when quota data is unavailable. This
// helper keeps the parsing local to this file so we do not rely on the generic
// parser which discarded the sign information and incorrectly treated "-1" as
// "1". When the headers provide multiple windows (per-second, per-minute, etc.)
// we use the window with the largest quota, matching the minute-level gauges we
// surface in monitoring.
func computeKucoinUsedWeight(header http.Header) int64 {
	limitVals := extractSignedInts(header.Get("gw-ratelimit-limit"))
	remainingVals := extractSignedInts(header.Get("gw-ratelimit-remaining"))

	limit, remaining := selectKucoinLimitWindow(limitVals, remainingVals)
	if limit <= 0 {
		// Without a positive limit we cannot determine usage. Treat this
		// as zero consumption so dashboards remain calm until we receive
		// real values from subsequent responses.
		return 0
	}

	used := limit - remaining
	if used < 0 {
		return 0
	}
	return used
}

// extractSignedInts returns all contiguous signed integers contained in s. It
// preserves the sign information so that sentinel values like "-1" are handled
// correctly. Empty or unparsable tokens are ignored.
func extractSignedInts(s string) []int64 {
	nums := make([]int64, 0, 4)
	if s == "" {
		return nums
	}

	var builder strings.Builder
	flush := func() {
		if builder.Len() == 0 {
			return
		}
		token := builder.String()
		builder.Reset()
		if token == "-" {
			return
		}
		if n, err := strconv.ParseInt(token, 10, 64); err == nil {
			nums = append(nums, n)
		}
	}

	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-':
			if builder.Len() > 0 {
				flush()
			}
			builder.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return nums
}

// selectKucoinLimitWindow chooses the most relevant limit/remaining pair from
// the parsed header values. KuCoin often reports multiple windows (per-second,
// per-minute, etc.). We pick the entry with the largest positive limit since it
// represents the minute-level pool we alert on. When the remaining quota is
// missing or negative, we fall back to the limit itself so the used weight
// evaluates to zero.
func selectKucoinLimitWindow(limitVals, remainingVals []int64) (limit int64, remaining int64) {
	if len(limitVals) == 0 {
		if len(remainingVals) == 0 {
			return 0, 0
		}
		remaining = remainingVals[0]
		if remaining < 0 {
			remaining = 0
		}
		return 0, remaining
	}

	bestIdx := -1
	var bestLimit int64
	for i, v := range limitVals {
		if v <= 0 {
			continue
		}
		if bestIdx == -1 || v > bestLimit {
			bestIdx = i
			bestLimit = v
		}
	}
	if bestIdx == -1 {
		bestIdx = 0
		bestLimit = limitVals[0]
	}

	limit = bestLimit
	if limit < 0 {
		limit = 0
	}

	if len(remainingVals) == 0 {
		remaining = limit
		return limit, remaining
	}

	remIdx := bestIdx
	if remIdx >= len(remainingVals) {
		remIdx = 0
	}
	remaining = remainingVals[remIdx]
	if remaining < 0 {
		remaining = limit
	}
	if remaining > limit {
		remaining = limit
	}
	return limit, remaining
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

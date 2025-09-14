package reader

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	futures "github.com/adshao/go-binance/v2/futures"

	"cryptoflow/logger"
)

// FetchRequestWeightLimit queries Binance exchangeInfo endpoint to retrieve the
// REQUEST_WEIGHT per minute limit. It returns 0 if the limit cannot be
// determined.
func FetchRequestWeightLimit(ctx context.Context, client *futures.Client) (int64, error) {
	info, err := client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return 0, err
	}
	for _, rl := range info.RateLimits {
		if rl.RateLimitType == "REQUEST_WEIGHT" && rl.Interval == "MINUTE" {
			return rl.Limit, nil
		}
	}
	return 0, nil
}

// depthSnapshotWeight returns the request weight for the depth endpoint based
// on the requested level limit. According to Binance docs the maximum 1000
// levels cost 20 weight. The weight scales with the depth requested.
func depthSnapshotWeight(limit int) int64 {
	switch {
	case limit <= 100:
		return 1
	case limit <= 500:
		return 5
	case limit <= 1000:
		return 20
	default:
		return 20
	}
}

// ReportSnapshotWeight parses the used weight from the HTTP response headers
// and emits metrics for weight usage.
func ReportSnapshotWeight(log *logger.Log, header http.Header, rateLimit int64, depthLimit int) {
	usedStr := header.Get("X-MBX-USED-WEIGHT-1m")
	used, _ := strconv.ParseInt(usedStr, 10, 64)
	remaining := rateLimit - used
	endpointWeight := depthSnapshotWeight(depthLimit)

	l := log.WithComponent("binance_reader")
	l.LogMetric("binance_reader", "used_weight", used, "gauge", logger.Fields{})
	l.LogMetric("binance_reader", "remaining_weight", remaining, "gauge", logger.Fields{})
	l.LogMetric("binance_reader", "endpoint_weight", endpointWeight, "gauge", logger.Fields{"limit": depthLimit})
}

// WSWeightTracker tracks the number of outgoing websocket messages and
// connection attempts for Binance futures depth streams.
type WSWeightTracker struct {
	mu       sync.Mutex
	window   time.Time
	msgs     int
	attempts int
}

// NewWSWeightTracker creates a new tracker.
func NewWSWeightTracker() *WSWeightTracker {
	return &WSWeightTracker{window: time.Now()}
}

// RegisterOutgoing records n outgoing client messages (subs/pings).
func (t *WSWeightTracker) RegisterOutgoing(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.window) >= time.Second {
		t.msgs = 0
		t.window = now
	}
	t.msgs += n
}

// RegisterConnectionAttempt records a websocket handshake attempt. Each
// attempt consumes weight on Binance side.
func (t *WSWeightTracker) RegisterConnectionAttempt() {
	t.mu.Lock()
	t.attempts++
	t.mu.Unlock()
}

// Stats returns the current message count within the one second window and the
// total connection attempts.
func (t *WSWeightTracker) Stats() (msgs int, attempts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	msgs = t.msgs
	attempts = t.attempts
	return
}

// ReportWSWeight emits websocket related weight metrics.
func ReportWSWeight(log *logger.Log, t *WSWeightTracker) {
	msgs, attempts := t.Stats()
	l := log.WithComponent("binance_delta_reader")
	l.LogMetric("binance_delta_reader", "outgoing_messages", int64(msgs), "gauge", logger.Fields{})
	l.LogMetric("binance_delta_reader", "connection_attempts", int64(attempts), "counter", logger.Fields{})
}

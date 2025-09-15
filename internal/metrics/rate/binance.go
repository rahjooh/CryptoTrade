package rate

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

// ReportSnapshotWeight parses the used weight from the HTTP response headers
// and emits a single `used_weight` metric for the given IP address.
func ReportSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	usedStr := header.Get("X-MBX-USED-WEIGHT-1m")
	used, _ := strconv.ParseInt(usedStr, 10, 64)

	l := log.WithComponent("binance_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("binance_reader", "used_weight", used, "gauge", fields)
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
func ReportWSWeight(log *logger.Log, t *WSWeightTracker, ip string) {
	msgs, attempts := t.Stats()
	l := log.WithComponent("binance_delta_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("binance_delta_reader", "outgoing_messages", int64(msgs), "gauge", fields)
	l.LogMetric("binance_delta_reader", "connection_attempts", int64(attempts), "counter", fields)
}

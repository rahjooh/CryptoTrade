package okxmetrics

import (
	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
	"net/http"
	"strconv"
	"strings"
)

const (
	SnapshotLimitPerWindow   = 10.0
	SnapshotWindowSeconds    = 2.0
	SnapshotWeightPerRequest = 1.0
	WebsocketConnLimitPerSec = 3.0
)

// RateLimitSnapshot captures the rate-limit metadata returned by OKX.
type RateLimitSnapshot struct {
	Limit        float64
	Remaining    float64
	ResetUnixMs  float64
	WindowSecond float64
}

// ExtractRateLimit pulls the rate-limit headers from the OKX REST response.
// Missing values fall back to the documented defaults (10 requests per 2s).
func ExtractRateLimit(header http.Header) RateLimitSnapshot {
	rl := RateLimitSnapshot{Limit: SnapshotLimitPerWindow, WindowSecond: SnapshotWindowSeconds}
	if header == nil {
		return rl
	}

	if v := header.Get("Rate-Limit-Limit"); v != "" {
		rl.Limit = parseFloat(v, SnapshotLimitPerWindow)
	}
	if v := header.Get("Rate-Limit-Remaining"); v != "" {
		rl.Remaining = parseFloat(v, 0)
	}
	if v := header.Get("Rate-Limit-Reset"); v != "" {
		rl.ResetUnixMs = parseFloat(v, 0)
		if rl.ResetUnixMs > 0 && rl.ResetUnixMs < 1e12 {
			rl.ResetUnixMs *= 1000
		}
	}
	if v := header.Get("Rate-Limit-Interval"); v != "" {
		if secs := parseIntervalSeconds(v); secs > 0 {
			rl.WindowSecond = secs
		}
	}
	return rl
}

// ReportUsage emits a single CloudWatch metric (used_weight) combining REST snapshots and websocket updates.
func ReportUsage(log *logger.Log, component, symbol, market, ip string, rl RateLimitSnapshot, weightPerCall, estimatedExtra float64) bool {
	if log == nil {
		return false
	}

	fields := logger.Fields{
		"exchange": "okx",
		"symbol":   symbol,
		"market":   market,
	}
	if ip != "" {
		fields["ip"] = ip
	}
	var (
		totalWeight float64
		hasWeight   bool
	)

	if rl.Limit > 0 && rl.Remaining >= 0 && weightPerCall > 0 {
		usedRequests := rl.Limit - rl.Remaining
		if usedRequests < 0 {
			usedRequests = 0
		}
		totalWeight = usedRequests * weightPerCall
		hasWeight = true
	}

	if estimatedExtra > 0 {
		if !hasWeight {
			totalWeight = 0
			hasWeight = true
		}
		totalWeight += estimatedExtra
	}

	if !hasWeight {
		return false
	}

	metrics.EmitMetric(log, component, "used_weight", totalWeight, "gauge", fields)
	return true
}

// EstimateSnapshotWeightPerMinute converts polling frequency into weight per minute.
func EstimateSnapshotWeightPerMinute(symbolCount, intervalMs int, weightPerCall float64) float64 {
	if symbolCount <= 0 || intervalMs <= 0 || weightPerCall <= 0 {
		return 0
	}
	perSymbolPerSecond := 1000.0 / float64(intervalMs)
	if perSymbolPerSecond <= 0 {
		return 0
	}
	return perSymbolPerSecond * float64(symbolCount) * 60 * weightPerCall
}

// EstimateWebsocketConnectionPressure returns the per-minute connection budget per active connection.
func EstimateWebsocketConnectionPressure(activeConnections int) float64 {
	if activeConnections <= 0 {
		return 0
	}
	return WebsocketConnLimitPerSec * 60 / float64(activeConnections)
}

func parseFloat(val string, fallback float64) float64 {
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	return fallback
}

func parseIntervalSeconds(val string) float64 {
	lower := strings.ToLower(strings.TrimSpace(val))
	if strings.HasSuffix(lower, "ms") {
		if f, err := strconv.ParseFloat(strings.TrimSuffix(lower, "ms"), 64); err == nil {
			return f / 1000
		}
	}
	if strings.HasSuffix(lower, "s") {
		if f, err := strconv.ParseFloat(strings.TrimSuffix(lower, "s"), 64); err == nil {
			return f
		}
	}
	return parseFloat(lower, 0)
}

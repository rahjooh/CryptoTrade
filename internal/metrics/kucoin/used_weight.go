package kucoinmetrics

import "cryptoflow/logger"

const (
	// FullSnapshotWeight represents KuCoin's documented weight for the futures
	// level2 snapshot endpoint.
	FullSnapshotWeight = 3.0
	// PartialDepthWeight represents the documented weight for partial depth
	// endpoints (level2 depth20/100). Useful when estimating REST usage beyond
	// snapshot polling.
	PartialDepthWeight = 5.0
)

// RateLimitSnapshot captures the rate-limit metadata needed for metrics.
type RateLimitSnapshot struct {
	Limit     int64
	Remaining int64
	Reset     int64 // milliseconds since epoch
}

// ReportUsage logs KuCoin REST usage and emits weight-oriented metrics. The
// function returns true when metrics are emitted.
func ReportUsage(
	log *logger.Log,
	component, symbol, market, ip string,
	rl RateLimitSnapshot,
	weightPerCall float64,
	estimatedExtra float64,
) bool {
	if log == nil {
		return false
	}

	fields := logger.Fields{
		"exchange": "kucoin",
		"symbol":   symbol,
		"market":   market,
	}
	if ip != "" {
		fields["ip"] = ip
	}

	emitted := false

	if rl.Limit > 0 {
		log.LogMetric(component, "request_weight_limit", float64(rl.Limit), "gauge", fields)
		emitted = true
	}
	if rl.Remaining >= 0 {
		log.LogMetric(component, "request_weight_remaining", float64(rl.Remaining), "gauge", fields)
		emitted = true
	}

	if rl.Limit > 0 && rl.Remaining >= 0 {
		used := float64(rl.Limit - rl.Remaining)
		if used < 0 {
			used = 0
		}
		if weightPerCall > 0 {
			log.LogMetric(component, "used_weight", used*weightPerCall, "gauge", fields)
		} else {
			log.LogMetric(component, "requests_used", used, "gauge", fields)
		}
		emitted = true
	}

	if estimatedExtra > 0 {
		log.LogMetric(component, "used_weight_estimated_extra", estimatedExtra, "gauge", fields)
		if rl.Limit > 0 && rl.Remaining >= 0 && weightPerCall > 0 {
			used := float64(rl.Limit-rl.Remaining) * weightPerCall
			log.LogMetric(component, "used_weight_total_estimate", used+estimatedExtra, "gauge", fields)
		}
		emitted = true
	}

	if rl.Reset > 0 {
		log.LogMetric(component, "limit_resets_at_unix_ms", float64(rl.Reset), "gauge", fields)
		emitted = true
	}

	if weightPerCall > 0 {
		log.LogMetric(component, "weight_per_call", weightPerCall, "gauge", fields)
		emitted = true
	}

	return emitted
}

// EstimateWeightPerMinute converts call frequency into estimated weight per
// minute using the documented request weight per call.
func EstimateWeightPerMinute(symbolCount, intervalMs int, weightPerCall float64) float64 {
	if symbolCount <= 0 || intervalMs <= 0 || weightPerCall <= 0 {
		return 0
	}

	callsPerSymbolPerSecond := 1000.0 / float64(intervalMs)
	callsPerSecond := callsPerSymbolPerSecond * float64(symbolCount)
	return callsPerSecond * 60 * weightPerCall
}

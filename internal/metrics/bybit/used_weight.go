package bybitmetrics

import (
	"net/http"
	"strconv"
	"time"

	"cryptoflow/logger"
)

// ReportUsage extracts Bybit REST rate-limit headers and emits the
// corresponding CloudWatch metrics. It returns the parsed limit and remaining
// quota alongside a boolean indicating that metrics were emitted.
func ReportUsage(log *logger.Log, resp *http.Response, component, symbol, market, ip string) (limit, remaining float64, emitted bool) {
	if log == nil || resp == nil {
		return 0, 0, false
	}

	headerLimit := resp.Header.Get("X-Bapi-Limit")
	headerRemaining := resp.Header.Get("X-Bapi-Limit-Status")
	if headerLimit == "" && headerRemaining == "" {
		return 0, 0, false
	}

	fields := logger.Fields{
		"exchange": "bybit",
		"symbol":   symbol,
		"market":   market,
	}
	if ip != "" {
		fields["ip"] = ip
	}

	if headerLimit != "" {
		if parsedLimit, err := strconv.ParseFloat(headerLimit, 64); err == nil {
			limit = parsedLimit
			log.LogMetric(component, "requests_limit", limit, "gauge", fields)
		} else {
			log.WithComponent(component).WithFields(logger.Fields{
				"header": "X-Bapi-Limit",
				"value":  headerLimit,
			}).WithError(err).Debug("failed to parse bybit limit header")
		}
	}

	if headerRemaining != "" {
		if parsedRemaining, err := strconv.ParseFloat(headerRemaining, 64); err == nil {
			remaining = parsedRemaining
			log.LogMetric(component, "requests_remaining", remaining, "gauge", fields)
		} else {
			log.WithComponent(component).WithFields(logger.Fields{
				"header": "X-Bapi-Limit-Status",
				"value":  headerRemaining,
			}).WithError(err).Debug("failed to parse bybit remaining header")
		}
	}

	if limit > 0 && remaining >= 0 {
		used := limit - remaining
		if used < 0 {
			used = 0
		}
		log.LogMetric(component, "requests_used", used, "gauge", fields)
	}

	if reset := resp.Header.Get("X-Bapi-Limit-Reset-Timestamp"); reset != "" {
		if ts, err := strconv.ParseInt(reset, 10, 64); err == nil {
			// Header is documented as milliseconds timestamp
			resetTime := time.UnixMilli(ts)
			log.LogMetric(component, "limit_resets_at_unix_ms", float64(resetTime.UnixMilli()), "gauge", fields)
		} else {
			log.WithComponent(component).WithFields(logger.Fields{
				"header": "X-Bapi-Limit-Reset-Timestamp",
				"value":  reset,
			}).WithError(err).Debug("failed to parse bybit reset header")
		}
	}

	return limit, remaining, true
}

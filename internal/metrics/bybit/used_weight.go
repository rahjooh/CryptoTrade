package bybitmetrics

import (
	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
	"net/http"
	"strconv"
	"strings"
)

// ReportUsage extracts Bybit REST rate-limit headers and emits the
// corresponding CloudWatch metrics. It returns the parsed limit and remaining
// quota alongside a boolean indicating that metrics were emitted.
func ReportUsage(log *logger.Log, resp *http.Response, component, symbol, market, ip string) (limit, remaining float64, emitted bool) {
	if log == nil || resp == nil {
		return 0, 0, false
	}
	if strings.TrimSpace(ip) == "" {
		log.WithComponent(component).WithField("symbol", symbol).Debug("skipping used weight metric; IP not provided [todo]")
		return 0, 0, false
	}

	headerLimit := resp.Header.Get("X-Bapi-Limit")
	headerRemaining := resp.Header.Get("X-Bapi-Limit-Status")
	if headerLimit == "" && headerRemaining == "" {
		return 0, 0, false
	}

	fields := logger.Fields{}
	if ip != "" {
		fields["ip"] = ip
	}

	if headerLimit != "" {
		if parsedLimit, err := strconv.ParseFloat(headerLimit, 64); err == nil {
			limit = parsedLimit
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
		metrics.EmitMetric(log, component, "used_weight", used, "gauge", fields)
	}

	return limit, remaining, true
}

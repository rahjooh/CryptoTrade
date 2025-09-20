package bybitmetrics

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

// ReportUsage extracts Bybit REST rate-limit headers and emits the
// corresponding CloudWatch metrics. It returns the parsed limit and remaining
// quota alongside a boolean indicating that metrics were emitted.
func ReportUsage(log *logger.Log, resp *http.Response, component, symbol, market, ip string) (limit, remaining float64, emitted bool) {
	if log == nil || resp == nil {
		return 0, 0, false
	}
	if strings.TrimSpace(ip) == "" {
		log.WithComponent(component).WithField("symbol", symbol).Debug("skipping used weight metric; IP not provided")
		return 0, 0, false
	}

	headerLimit := resp.Header.Get("X-Bapi-Limit")
	if headerLimit == "" {
		headerLimit = resp.Header.Get("X-Bapi-Limit-Quota")
	}
	headerRemaining := resp.Header.Get("X-Bapi-Limit-Status")
	if headerRemaining == "" {
		headerRemaining = resp.Header.Get("X-Bapi-Limit-Remaining")
	}
	if headerLimit == "" && headerRemaining == "" {
		log.WithComponent(component).WithFields(logger.Fields{
			"symbol": symbol,
			"market": market,
		}).Debug("missing bybit rate limit headers")
		return 0, 0, false
	}

	fields := logger.Fields{}
	if ip != "" {
		fields["ip"] = ip
	}

	limit, limitOK := parseLimit(headerLimit, headerRemaining)
	remaining, remainingOK := parseRemaining(headerLimit, headerRemaining)

	if !limitOK || !remainingOK {
		log.WithComponent(component).WithFields(logger.Fields{
			"symbol":           symbol,
			"market":           market,
			"limit_header":     headerLimit,
			"remaining_header": headerRemaining,
		}).Debug("unable to determine bybit rate limit numbers")
		return limit, remaining, false
	}

	if limit > 0 && remaining >= 0 {
		used := limit - remaining
		if used < 0 {
			used = 0
		}
		log.WithComponent(component).WithFields(logger.Fields{
			"symbol":    symbol,
			"market":    market,
			"limit":     limit,
			"remaining": remaining,
			"used":      used,
		}).Debug("calculated bybit used weight")
		metrics.EmitMetric(log, component, "used_weight", used, "gauge", fields)
	}

	return limit, remaining, true
}

var numberPattern = regexp.MustCompile(`[0-9]+(?:\.[0-9]+)?`)

func parseLimit(limitHeader, statusHeader string) (float64, bool) {
	if v, ok := firstNumber(limitHeader); ok {
		return v, true
	}
	if numbers := allNumbers(statusHeader); len(numbers) > 1 {
		return numbers[1], true
	}
	return 0, false
}

func parseRemaining(limitHeader, statusHeader string) (float64, bool) {
	if v, ok := firstNumber(statusHeader); ok {
		return v, true
	}
	if numbers := allNumbers(limitHeader); len(numbers) > 1 {
		return numbers[1], true
	}
	return 0, false
}

func firstNumber(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}
	numbers := allNumbers(raw)
	if len(numbers) == 0 {
		return 0, false
	}
	return numbers[0], true
}

func allNumbers(raw string) []float64 {
	if raw == "" {
		return nil
	}
	matches := numberPattern.FindAllString(raw, -1)
	if len(matches) == 0 {
		return nil
	}
	values := make([]float64, 0, len(matches))
	for _, m := range matches {
		if v, err := strconv.ParseFloat(m, 64); err == nil {
			values = append(values, v)
		}
	}
	return values
}

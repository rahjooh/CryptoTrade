package rate

import (
	"fmt"
	"strings"

	"cryptoflow/logger"
)

// ReportRateLimitExceeded increments the rate limit exceeded counter for the given
// exchange and data type and emits the metric to CloudWatch. Additional fields such as
// exchange, symbol, ip and type are attached to the log entry.
func ReportRateLimitExceeded(log *logger.Log, exchange, symbol, ip, dataType string) {
	component := fmt.Sprintf("%s_%s", strings.ToLower(exchange), strings.ToLower(dataType))
	l := log.WithComponent(component)
	fields := logger.Fields{
		"exchange": strings.ToLower(exchange),
		"symbol":   symbol,
		"ip":       ip,
		"type":     strings.ToLower(dataType),
	}
	l.LogMetric(component, "rate_limit_exceeded", int64(1), "counter", fields)
	l.WithFields(fields).Warn("rate limit exceeded")
}

// ReportIPBan increments the IP ban counter for the given exchange and data type and emits
// the metric to CloudWatch. Additional fields such as exchange, symbol, ip and type are
// attached to the log entry.
func ReportIPBan(log *logger.Log, exchange, symbol, ip, dataType string) {
	component := fmt.Sprintf("%s_%s", strings.ToLower(exchange), strings.ToLower(dataType))
	l := log.WithComponent(component)
	fields := logger.Fields{
		"exchange": strings.ToLower(exchange),
		"symbol":   symbol,
		"ip":       ip,
		"type":     strings.ToLower(dataType),
	}
	l.LogMetric(component, "ip_ban", int64(1), "counter", fields)
	l.WithFields(fields).Error("ip banned")
}

// detectLimit inspects the message returned from an exchange and determines whether
// it signals a rate limit exceed or an IP ban. The detection logic is customised per
// exchange as each one uses different wording.
func detectLimit(exchange, msg string) (rateLimit bool, ipBan bool) {
	lowerMsg := strings.ToLower(msg)
	switch strings.ToLower(exchange) {
	case "binance":
		rateLimit = strings.Contains(lowerMsg, "too many requests") || strings.Contains(lowerMsg, "rate limit")
		ipBan = strings.Contains(lowerMsg, "ip") && strings.Contains(lowerMsg, "ban")
	case "okx":
		rateLimit = strings.Contains(lowerMsg, "too many requests") || strings.Contains(lowerMsg, "frequency limit")
		ipBan = strings.Contains(lowerMsg, "ip") && (strings.Contains(lowerMsg, "blocked") || strings.Contains(lowerMsg, "ban"))
	case "kucoin":
		rateLimit = strings.Contains(lowerMsg, "too many requests") || strings.Contains(lowerMsg, "rate limit")
		ipBan = strings.Contains(lowerMsg, "ip") && strings.Contains(lowerMsg, "limit") && strings.Contains(lowerMsg, "triggered")
	case "bybit":
		ipBan = strings.Contains(lowerMsg, "ip rate limit") || (strings.Contains(lowerMsg, "ip") && strings.Contains(lowerMsg, "ban"))
		rateLimit = !ipBan && (strings.Contains(lowerMsg, "rate limit") || strings.Contains(lowerMsg, "too many requests") || strings.Contains(lowerMsg, "too many visits"))
	default:
		rateLimit = strings.Contains(lowerMsg, "rate limit") || strings.Contains(lowerMsg, "too many requests")
		ipBan = strings.Contains(lowerMsg, "ip") && strings.Contains(lowerMsg, "ban")
	}
	return
}

// ReportLimitFromMessage checks the provided message for rate limit or IP ban events
// based on exchange-specific keywords and records the appropriate metrics. No action
// is taken if the message does not match any known patterns.
func ReportLimitFromMessage(log *logger.Log, exchange, symbol, ip, dataType, msg string) {
	rateLimit, ipBan := detectLimit(exchange, msg)
	if rateLimit {
		ReportRateLimitExceeded(log, exchange, symbol, ip, dataType)
	}
	if ipBan {
		ReportIPBan(log, exchange, symbol, ip, dataType)
	}
}

package binancemetrics

import (
	"net/http"
	"strconv"

	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

// ReportUsedWeight inspects Binance used-weight headers and emits metrics when a
// numeric value is found. The function returns the parsed weight and a boolean
// indicating whether a metric was recorded. When estimatedExtra is greater than
// zero the emitted metric includes the websocket-derived load so CloudWatch
// observes a single combined gauge.
func ReportUsedWeight(log *logger.Log, resp *http.Response, component, symbol, market, ip string, estimatedExtra float64) (float64, bool) {
	if log == nil || resp == nil {
		return 0, false
	}

	headers := []struct {
		key    string
		window string
	}{
		{"X-MBX-USED-WEIGHT-1M", "1m"},
		{"X-MBX-USED-WEIGHT", "1m"},
		{"X-MBX-USED-WEIGHT-1S", "1s"},
	}

	for _, h := range headers {
		value := resp.Header.Get(h.key)
		if value == "" {
			continue
		}

		used, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.WithComponent(component).WithFields(logger.Fields{
				"symbol": symbol,
				"header": h.key,
				"value":  value,
			}).WithError(err).Debug("failed to parse used weight header")
			continue
		}

		fields := logger.Fields{
			"exchange": "binance",
			"symbol":   symbol,
			"market":   market,
			"window":   h.window,
		}
		if ip != "" {
			fields["ip"] = ip
		}

		total := used

		if estimatedExtra > 0 {
			total += estimatedExtra
		}
		metrics.EmitMetric(log, component, "used_weight", total, "gauge", fields)
		return used, true
	}

	return 0, false
}

// EstimateWebsocketWeightPerMinute approximates the number of websocket
// diff-depth messages received per minute and treats each message as a unit of
// weight. The return value can be divided among symbols or used wholesale to
// project additional load for monitoring purposes.
func EstimateWebsocketWeightPerMinute(symbolCount, intervalMs int) float64 {
	if symbolCount <= 0 || intervalMs <= 0 {
		return 0
	}

	updatesPerSymbolPerSecond := 1000.0 / float64(intervalMs)
	if updatesPerSymbolPerSecond <= 0 {
		return 0
	}

	updatesPerSecond := updatesPerSymbolPerSecond * float64(symbolCount)
	return updatesPerSecond * 60
}

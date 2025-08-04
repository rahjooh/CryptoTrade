// Registers:
//
//	#CryptoFlow_snapshot_success_total
//	#CryptoFlow_snapshot_errors_total
//	#go_* and process_* system metrics
//
// Exposes them on :2112/metrics using Prometheus HTTP handler
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	once            sync.Once
	snapshotSuccess *prometheus.CounterVec
	snapshotErrors  *prometheus.CounterVec
)

func Init() {
	once.Do(func() {
		snapshotSuccess = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "CryptoFlow_snapshot_success_total",
				Help: "Number of successful orderbook snapshots written",
			},
			[]string{"symbol"},
		)

		snapshotErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "CryptoFlow_snapshot_errors_total",
				Help: "Number of failed fetch/write attempts",
			},
			[]string{"symbol"},
		)

		_ = prometheus.Register(snapshotSuccess)
		_ = prometheus.Register(snapshotErrors)
		_ = prometheus.Register(collectors.NewGoCollector())
		_ = prometheus.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe("0.0.0.0:2112", nil); err != nil {
				panic("metrics server failed: " + err.Error())
			}
		}()
	})
}

// IncrementSuccess increases the success counter for a given symbol.
func IncrementSuccess(symbol string) {
	if snapshotSuccess != nil {
		snapshotSuccess.WithLabelValues(symbol).Inc()
	}
}

// IncrementError increases the error counter for a given symbol.
func IncrementError(symbol string) {
	if snapshotErrors != nil {
		snapshotErrors.WithLabelValues(symbol).Inc()
	}
}

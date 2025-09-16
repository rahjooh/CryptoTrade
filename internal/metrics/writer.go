package metrics

import "cryptoflow/logger"

// WriterStats holds metrics for writer components.
type WriterStats struct {
	BatchesWritten int64
	FilesWritten   int64
	BytesWritten   int64
	ErrorsCount    int64
	NormChannelLen int
	NormChannelCap int
}

// ReportWriter emits common writer metrics using the provided logger and component name.
func ReportWriter(log *logger.Log, component string, stats WriterStats) {
	l := log.WithComponent(component)

	errorRate := float64(0)
	if stats.BatchesWritten+stats.ErrorsCount > 0 {
		errorRate = float64(stats.ErrorsCount) / float64(stats.BatchesWritten+stats.ErrorsCount)
	}

	avgBytesPerFile := float64(0)
	if stats.FilesWritten > 0 {
		avgBytesPerFile = float64(stats.BytesWritten) / float64(stats.FilesWritten)
	}

	l.LogMetric(component, "batches_written", stats.BatchesWritten, "counter", logger.Fields{})
	l.LogMetric(component, "files_written", stats.FilesWritten, "counter", logger.Fields{})
	l.LogMetric(component, "bytes_written", stats.BytesWritten, "counter", logger.Fields{})
	l.LogMetric(component, "errors_count", stats.ErrorsCount, "counter", logger.Fields{})
	l.LogMetric(component, "error_rate", errorRate, "gauge", logger.Fields{})
	l.LogMetric(component, "avg_bytes_per_file", avgBytesPerFile, "gauge", logger.Fields{})
	l.LogMetric(component, "norm_channel_len", stats.NormChannelLen, "gauge", logger.Fields{})

	entry := l.WithFields(logger.Fields{
		"batches_written":    stats.BatchesWritten,
		"files_written":      stats.FilesWritten,
		"bytes_written":      stats.BytesWritten,
		"errors_count":       stats.ErrorsCount,
		"error_rate":         errorRate,
		"avg_bytes_per_file": avgBytesPerFile,
		"norm_channel_len":   stats.NormChannelLen,
		"norm_channel_cap":   stats.NormChannelCap,
	})

	if stats.ErrorsCount > 0 {
		entry.Warn(component + " metrics")
		return
	}

	entry.Info(component + " metrics")
}

package metrics

import "cryptoflow/logger"

// fobd_proccesor_metrics holds channel size metrics for the delta processor.
type fobd_proccesor_metrics struct {
	RawLen  int
	RawCap  int
	NormLen int
	NormCap int
}

// report_fobd_proccesor_metrics emits channel size metrics for the delta processor.
func report_fobd_proccesor_metrics(log *logger.Log, sizes fobd_proccesor_metrics) {
	log.WithComponent("delta_processor").WithFields(logger.Fields{
		"raw_channel_len":  sizes.RawLen,
		"raw_channel_cap":  sizes.RawCap,
		"norm_channel_len": sizes.NormLen,
		"norm_channel_cap": sizes.NormCap,
	}).Info("delta processor channel sizes")
}

// fobs_proccesor_metrics holds metrics for the flattener processor.
type fobs_proccesor_metrics struct {
	MessagesProcessed int64
	BatchesProcessed  int64
	EntriesProcessed  int64
	ErrorsCount       int64
	ActiveBatches     int
	RawChannelLen     int
	RawChannelCap     int
	NormChannelLen    int
	NormChannelCap    int
}

// report_fobs_proccesor_metrics emits metrics for the flattener component.
func report_fobs_proccesor_metrics(log *logger.Log, stats fobs_proccesor_metrics) {
	l := log.WithComponent("flattener")

	errorRate := float64(0)
	if stats.MessagesProcessed+stats.ErrorsCount > 0 {
		errorRate = float64(stats.ErrorsCount) / float64(stats.MessagesProcessed+stats.ErrorsCount)
	}

	avgEntriesPerMessage := float64(0)
	if stats.MessagesProcessed > 0 {
		avgEntriesPerMessage = float64(stats.EntriesProcessed) / float64(stats.MessagesProcessed)
	}

	l.LogMetric("flattener", "messages_processed", stats.MessagesProcessed, "counter", logger.Fields{})
	l.LogMetric("flattener", "batches_processed", stats.BatchesProcessed, "counter", logger.Fields{})
	l.LogMetric("flattener", "entries_processed", stats.EntriesProcessed, "counter", logger.Fields{})
	l.LogMetric("flattener", "errors_count", stats.ErrorsCount, "counter", logger.Fields{})
	l.LogMetric("flattener", "error_rate", errorRate, "gauge", logger.Fields{})
	l.LogMetric("flattener", "active_batches", stats.ActiveBatches, "gauge", logger.Fields{})
	l.LogMetric("flattener", "avg_entries_per_message", avgEntriesPerMessage, "gauge", logger.Fields{})

	l.WithFields(logger.Fields{
		"messages_processed":      stats.MessagesProcessed,
		"batches_processed":       stats.BatchesProcessed,
		"entries_processed":       stats.EntriesProcessed,
		"errors_count":            stats.ErrorsCount,
		"error_rate":              errorRate,
		"active_batches":          stats.ActiveBatches,
		"avg_entries_per_message": avgEntriesPerMessage,
		"raw_channel_len":         stats.RawChannelLen,
		"raw_channel_cap":         stats.RawChannelCap,
		"norm_channel_len":        stats.NormChannelLen,
		"norm_channel_cap":        stats.NormChannelCap,
	}).Info("flattener metrics")
}

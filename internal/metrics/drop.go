package metrics

import "cryptoflow/logger"

// DropMetric identifies the metric name emitted when channel messages are dropped.
type DropMetric string

const (
	// DropMetricSnapshotRaw records dropped snapshot messages before normalisation.
	DropMetricSnapshotRaw DropMetric = "snapshot_messages_dropped"
	// DropMetricDeltaRaw records dropped delta messages before normalisation.
	DropMetricDeltaRaw DropMetric = "delta_messages_dropped"
	// DropMetricDeltaNorm records dropped delta batches after normalisation.
	DropMetricDeltaNorm DropMetric = "delta_norm_messages_dropped"
	// DropMetricLiquidationRaw records dropped liquidation stream messages.
	DropMetricLiquidationRaw DropMetric = "liquidation_messages_dropped"
	// DropMetricOpenInterestRaw records dropped open interest stream messages.
	DropMetricOpenInterestRaw DropMetric = "open_interest_messages_dropped"
	// DropMetricPremiumIndexRaw records dropped premium index stream messages.
	DropMetricPremiumIndexRaw DropMetric = "premium_index_messages_dropped"
)

// EmitDropMetric logs and emits a metric representing a dropped channel message. The
// metric value is always incremented by one so callers should invoke this helper for
// each dropped message. Optional metadata (exchange, market, symbol, stage) is added
// to the metric fields when provided which enables downstream aggregation per
// exchange and stream type.
func EmitDropMetric(log *logger.Log, metric DropMetric, exchange, market, symbol, stage string) {
	fields := logger.Fields{}
	if exchange != "" {
		fields["exchange"] = exchange
	}
	if market != "" {
		fields["market"] = market
	}
	if symbol != "" {
		fields["symbol"] = symbol
	}
	if stage != "" {
		fields["stage"] = stage
	}

	EmitMetric(log, "channel_drops", string(metric), 1, "counter", fields)
}

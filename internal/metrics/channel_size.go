package metrics

import (
	"context"
	"time"

	"cryptoflow/internal/channel"
	"cryptoflow/logger"
)

// StartChannelSizeMetrics emits occupancy metrics for the snapshot (FOBS) and
// delta (FOBD) channel buffers. Metrics are logged every `interval` until the
// context is cancelled. When interval <=0, a one-second cadence is used.
func StartChannelSizeMetrics(ctx context.Context, channels *channel.Channels, interval time.Duration) {
	if !IsFeatureEnabled(FeatureChannelSize) {
		return
	}
	if channels == nil {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}

	log := logger.GetLogger()
	ticker := time.NewTicker(interval)
	component := "channel_buffers"

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if channels.FOBS != nil {
					EmitMetric(log, component, "fobs_raw_buffer_length", len(channels.FOBS.Raw), "gauge", logger.Fields{
						"buffer":   "fobs_raw",
						"capacity": cap(channels.FOBS.Raw),
					})
					EmitMetric(log, component, "fobs_norm_buffer_length", len(channels.FOBS.Norm), "gauge", logger.Fields{
						"buffer":   "fobs_norm",
						"capacity": cap(channels.FOBS.Norm),
					})
				}
				if channels.FOBD != nil {
					EmitMetric(log, component, "fobd_raw_buffer_length", len(channels.FOBD.Raw), "gauge", logger.Fields{
						"buffer":   "fobd_raw",
						"capacity": cap(channels.FOBD.Raw),
					})
					EmitMetric(log, component, "fobd_norm_buffer_length", len(channels.FOBD.Norm), "gauge", logger.Fields{
						"buffer":   "fobd_norm",
						"capacity": cap(channels.FOBD.Norm),
					})
				}
				if channels.Liq != nil {
					EmitMetric(log, component, "liq_raw_buffer_length", len(channels.Liq.Raw), "gauge", logger.Fields{
						"buffer":   "liq_raw",
						"capacity": cap(channels.Liq.Raw),
					})
				}
				if channels.OI != nil {
					EmitMetric(log, component, "oi_raw_buffer_length", len(channels.OI.Raw), "gauge", logger.Fields{
						"buffer":   "oi_raw",
						"capacity": cap(channels.OI.Raw),
					})
				}
			}
		}
	}()
}

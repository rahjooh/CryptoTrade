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
				if channels.FOI != nil {
					EmitMetric(log, component, "foi_raw_buffer_length", len(channels.FOI.Raw), "gauge", logger.Fields{
						"buffer":   "foi_raw",
						"capacity": cap(channels.FOI.Raw),
					})
					EmitMetric(log, component, "foi_norm_buffer_length", len(channels.FOI.Norm), "gauge", logger.Fields{
						"buffer":   "foi_norm",
						"capacity": cap(channels.FOI.Norm),
					})
				}
				if channels.Liq != nil {
					EmitMetric(log, component, "liq_raw_buffer_length", len(channels.Liq.Raw), "gauge", logger.Fields{
						"buffer":   "liq_raw",
						"capacity": cap(channels.Liq.Raw),
					})
					EmitMetric(log, component, "liq_norm_buffer_length", len(channels.Liq.Norm), "gauge", logger.Fields{
						"buffer":   "liq_norm",
						"capacity": cap(channels.Liq.Norm),
					})
				}
				if channels.PI != nil {
					EmitMetric(log, component, "pi_raw_buffer_length", len(channels.PI.Raw), "gauge", logger.Fields{
						"buffer":   "pi_raw",
						"capacity": cap(channels.PI.Raw),
					})
					EmitMetric(log, component, "pi_norm_buffer_length", len(channels.PI.Norm), "gauge", logger.Fields{
						"buffer":   "pi_norm",
						"capacity": cap(channels.PI.Norm),
					})
				}
			}
		}
	}()
}

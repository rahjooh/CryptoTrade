package fobd

import (
	"context"
	"sync"
	"time"

	"cryptoflow/logger"
	"cryptoflow/models"
)

type ChannelStats struct {
	RawSent     int64
	NormSent    int64
	RawDropped  int64
	NormDropped int64
}

type Channels struct {
	Raw  chan models.RawFOBDmodel
	Norm chan models.RawFOBDbatchModel

	stats               ChannelStats
	statsMutex          sync.RWMutex
	log                 *logger.Log
	ctx                 context.Context
	metricsReportTicker *time.Ticker
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	log := logger.GetLogger()
	c := &Channels{
		Raw:  make(chan models.RawFOBDmodel, rawBufferSize),
		Norm: make(chan models.RawFOBDbatchModel, normBufferSize),
		log:  log,
	}

	log.WithComponent("fobd_channels").WithFields(logger.Fields{
		"raw_buffer_size":  rawBufferSize,
		"norm_buffer_size": normBufferSize,
	}).Info("FOBD channels initialized")

	return c
}

func (c *Channels) startMetricsReporting(ctx context.Context) {
	c.ctx = ctx
	c.metricsReportTicker = time.NewTicker(30 * time.Second)

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.metricsReportticker.Stop()
				return
			case <-c.metricsReportTicker.C:
				c.logChannelStats(c.log)
			}
		}
	}()
}

func (c *Channels) logChannelStats(log *logger.Log) {
	c.statsMutex.RLock()
	stats := c.stats
	c.statsMutex.RUnlock()

	log.WithComponent("fobd_channels").WithFields(logger.Fields{
		"raw_sent":         stats.RawSent,
		"norm_sent":        stats.NormSent,
		"raw_dropped":      stats.RawDropped,
		"norm_dropped":     stats.NormDropped,
		"raw_channel_len":  len(c.Raw),
		"raw_channel_cap":  cap(c.Raw),
		"norm_channel_len": len(c.Norm),
		"norm_channel_cap": cap(c.Norm),
	}).Info("FOBD channel statistics")
}

func (c *Channels) Close() {
	if c.metricsReportTicker != nil {
		c.metricsReportticker.Stop()
	}
	close(c.Raw)
	close(c.Norm)
	c.log.WithComponent("fobd_channels").Info("FOBD channels closed")
}

func (c *Channels) IncrementRawSent() {
	c.statsMutex.Lock()
	c.stats.RawSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementNormSent() {
	c.statsMutex.Lock()
	c.stats.NormSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementRawDropped() {
	c.statsMutex.Lock()
	c.stats.RawDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementNormDropped() {
	c.statsMutex.Lock()
	c.stats.NormDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

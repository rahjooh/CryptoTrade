package internal

import (
	"context"
	"sync"
	"time"

	"cryptoflow/logger"
	"cryptoflow/models"
)

type ChannelStats struct {
	RawFOBSchSent     int64
	NormFOBSchSent    int64
	RawFOBSchDropped  int64
	NormFOBSchDropped int64
	RawFOBDchSent     int64
	NormFOBDchSent    int64
	RawFOBDchDropped  int64
	NormFOBDchDropped int64
}

type Channels struct {
	RawFOBSch  chan models.RawOrderbookMessage
	NormFOBSch chan models.FlattenedOrderbookBatch
	RawFOBDch  chan models.RawFOBDmodel
	NormFOBDch chan models.RawFOBDbatchModel

	stats               ChannelStats
	statsMutex          sync.RWMutex
	log                 *logger.Log
	ctx                 context.Context
	metricsReportTicker *time.Ticker
}

func NewChannels(rawBufferSize, flattenedBufferSize int) *Channels {
	log := logger.GetLogger()

	c := &Channels{
		RawFOBSch:  make(chan models.RawOrderbookMessage, rawBufferSize),
		NormFOBSch: make(chan models.FlattenedOrderbookBatch, flattenedBufferSize),
		RawFOBDch:  make(chan models.RawFOBDmodel, rawBufferSize),
		NormFOBDch: make(chan models.RawFOBDbatchModel, flattenedBufferSize),
		log:        log,
	}

	log.WithComponent("channels").WithFields(logger.Fields{
		"raw_buffer_size":       rawBufferSize,
		"flattened_buffer_size": flattenedBufferSize,
	}).Info("channels initialized")

	return c
}

func (c *Channels) StartMetricsReporting(ctx context.Context) {
	c.ctx = ctx
	c.metricsReportTicker = time.NewTicker(30 * time.Second)

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.metricsReportTicker.Stop()
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

	log.WithComponent("channels").WithFields(logger.Fields{
		"raw_FOBS_sent":         stats.RawFOBSchSent,
		"norm_FOBS_sent":        stats.NormFOBSchSent,
		"raw_FOBS_dropped":      stats.RawFOBSchDropped,
		"norm_FOBS_dropped":     stats.NormFOBSchDropped,
		"raw_FOBD_sent":         stats.RawFOBDchSent,
		"norm_FOBD_sent":        stats.NormFOBDchSent,
		"raw_FOBD_dropped":      stats.RawFOBDchDropped,
		"norm_FOBD_dropped":     stats.NormFOBDchDropped,
		"raw_channel_len":       len(c.RawFOBSch),
		"raw_channel_cap":       cap(c.RawFOBSch),
		"flattened_channel_len": len(c.NormFOBSch),
		"flattened_channel_cap": cap(c.NormFOBSch),
		"raw_fobd_channel_len":  len(c.RawFOBDch),
		"raw_fobd_channel_cap":  cap(c.RawFOBDch),
		"norm_fobd_channel_len": len(c.NormFOBDch),
		"norm_fobd_channel_cap": cap(c.NormFOBDch),
	}).Info("channel statistics")
}

func (c *Channels) Close() {
	if c.metricsReportTicker != nil {
		c.metricsReportTicker.Stop()
	}

	close(c.RawFOBSch)
	close(c.NormFOBSch)
	close(c.RawFOBDch)
	close(c.NormFOBDch)

	c.log.WithComponent("channels").Info("all channels closed")
}

func (c *Channels) IncrementRawFOBSchSent() {
	c.statsMutex.Lock()
	c.stats.RawFOBSchSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementNormFOBSchSent() {
	c.statsMutex.Lock()
	c.stats.NormFOBSchSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementRawFOBSchDropped() {
	c.statsMutex.Lock()
	c.stats.RawFOBSchDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementNormFOBSchDropped() {
	c.statsMutex.Lock()
	c.stats.NormFOBSchDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementRawFOBDchSent() {
	c.statsMutex.Lock()
	c.stats.RawFOBDchSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementNormFOBDchSent() {
	c.statsMutex.Lock()
	c.stats.NormFOBDchSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementRawFOBDchDropped() {
	c.statsMutex.Lock()
	c.stats.RawFOBDchDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementNormFOBDchDropped() {
	c.statsMutex.Lock()
	c.stats.NormFOBDchDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

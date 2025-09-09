package fobs

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
	Raw  chan models.RawFOBSMessage
	Norm chan models.BatchFOBSMessage

	stats               ChannelStats
	statsMutex          sync.RWMutex
	log                 *logger.Log
	ctx                 context.Context
	metricsReportTicker *time.Ticker
	depthReportTicker   *time.Ticker
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	log := logger.GetLogger()
	c := &Channels{
		Raw:  make(chan models.RawFOBSMessage, rawBufferSize),
		Norm: make(chan models.BatchFOBSMessage, normBufferSize),
		log:  log,
	}

	log.WithComponent("fobs_channels").WithFields(logger.Fields{
		"raw_buffer_size":  rawBufferSize,
		"norm_buffer_size": normBufferSize,
	}).Info("FOBS channels initialized")

	return c
}

func (c *Channels) StartMetricsReporting(ctx context.Context) {
	c.ctx = ctx
	c.metricsReportTicker = time.NewTicker(30 * time.Second)
	c.depthReportTicker = time.NewTicker(5 * time.Second)

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

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.depthReportTicker.Stop()
				return
			case <-c.depthReportTicker.C:
				logger.RecordChannelDepth("fobs_raw", len(c.Raw), cap(c.Raw))
				logger.RecordChannelDepth("fobs_norm", len(c.Norm), cap(c.Norm))
			}
		}
	}()
}

func (c *Channels) logChannelStats(log *logger.Log) {
	c.statsMutex.RLock()
	stats := c.stats
	c.statsMutex.RUnlock()

	log.WithComponent("fobs_channels").WithFields(logger.Fields{
		"raw_sent":         stats.RawSent,
		"norm_sent":        stats.NormSent,
		"raw_dropped":      stats.RawDropped,
		"norm_dropped":     stats.NormDropped,
		"raw_channel_len":  len(c.Raw),
		"raw_channel_cap":  cap(c.Raw),
		"norm_channel_len": len(c.Norm),
		"norm_channel_cap": cap(c.Norm),
	}).Info("FOBS channel statistics")
}

func (c *Channels) Close() {
	if c.metricsReportTicker != nil {
		c.metricsReportTicker.Stop()
	}
	if c.depthReportTicker != nil {
		c.depthReportTicker.Stop()
	}
	close(c.Raw)
	close(c.Norm)
	c.log.WithComponent("fobs_channels").Info("FOBS channels closed")
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
	logger.IncrementDroppedMessages()
}

func (c *Channels) IncrementNormDropped() {
	c.statsMutex.Lock()
	c.stats.NormDropped++
	c.statsMutex.Unlock()
	logger.IncrementDroppedMessages()
}

func (c *Channels) SendRaw(ctx context.Context, msg models.RawFOBSMessage) bool {
	select {
	case c.Raw <- msg:
		c.IncrementRawSent()
		return true
	case <-ctx.Done():
		return false
	default:
		c.IncrementRawDropped()
		return false
	}
}

func (c *Channels) SendNorm(ctx context.Context, msg models.BatchFOBSMessage) bool {
	select {
	case c.Norm <- msg:
		c.IncrementNormSent()
		return true
	case <-ctx.Done():
		return false
	default:
		c.IncrementNormDropped()
		return false
	}
}

func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

package internal

import (
	"context"
	"sync"
	"time"

	"cryptoflow/logger"
	"cryptoflow/models"
)

type ChannelStats struct {
	RawMessagesSent         int64
	FlattenedBatchesSent    int64
	RawMessagesDropped      int64
	FlattenedBatchesDropped int64
	RawFOBDSent             int64
	NormFOBDSent            int64
}

type Channels struct {
	RawMessageChan chan models.RawOrderbookMessage
	FlattenedChan  chan models.FlattenedOrderbookBatch
	RawFOBDChan    chan models.RawOrderbookDelta
	NormFOBDChan   chan models.OrderbookDeltaBatch

	stats               ChannelStats
	statsMutex          sync.RWMutex
	log                 *logger.Log
	ctx                 context.Context
	metricsReportTicker *time.Ticker
}

func NewChannels(rawBufferSize, flattenedBufferSize int) *Channels {
	log := logger.GetLogger()

	c := &Channels{
		RawMessageChan: make(chan models.RawOrderbookMessage, rawBufferSize),
		FlattenedChan:  make(chan models.FlattenedOrderbookBatch, flattenedBufferSize),
		RawFOBDChan:    make(chan models.RawOrderbookDelta, rawBufferSize),
		NormFOBDChan:   make(chan models.OrderbookDeltaBatch, flattenedBufferSize),
		log:            log,
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
		"raw_messages_sent":         stats.RawMessagesSent,
		"flattened_batches_sent":    stats.FlattenedBatchesSent,
		"raw_messages_dropped":      stats.RawMessagesDropped,
		"flattened_batches_dropped": stats.FlattenedBatchesDropped,
		"raw_channel_len":           len(c.RawMessageChan),
		"raw_channel_cap":           cap(c.RawMessageChan),
		"flattened_channel_len":     len(c.FlattenedChan),
		"flattened_channel_cap":     cap(c.FlattenedChan),
		"raw_fobd_channel_len":      len(c.RawFOBDChan),
		"raw_fobd_channel_cap":      cap(c.RawFOBDChan),
		"norm_fobd_channel_len":     len(c.NormFOBDChan),
		"norm_fobd_channel_cap":     cap(c.NormFOBDChan),
	}).Info("channel statistics")
}

func (c *Channels) Close() {
	if c.metricsReportTicker != nil {
		c.metricsReportTicker.Stop()
	}

	close(c.RawMessageChan)
	close(c.FlattenedChan)
	close(c.RawFOBDChan)
	close(c.NormFOBDChan)

	c.log.WithComponent("channels").Info("all channels closed")
}

func (c *Channels) IncrementRawMessagesSent() {
	c.statsMutex.Lock()
	c.stats.RawMessagesSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementFlattenedBatchesSent() {
	c.statsMutex.Lock()
	c.stats.FlattenedBatchesSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementRawMessagesDropped() {
	c.statsMutex.Lock()
	c.stats.RawMessagesDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementFlattenedBatchesDropped() {
	c.statsMutex.Lock()
	c.stats.FlattenedBatchesDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

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
	SortedBatchesSent       int64
	RawMessagesDropped      int64
	FlattenedBatchesDropped int64
	SortedBatchesDropped    int64
}

type Channels struct {
	RawMessageChan chan models.RawOrderbookMessage
	FlattenedChan  chan models.FlattenedOrderbookBatch
	SortedChan     chan models.SortedOrderbookBatch

	stats               ChannelStats
	statsMutex          sync.RWMutex
	log                 *logger.Logger
	ctx                 context.Context
	metricsReportTicker *time.Ticker
}

func NewChannels(rawBufferSize, flattenedBufferSize, sortedBufferSize int) *Channels {
	log := logger.GetLogger()

	c := &Channels{
		RawMessageChan: make(chan models.RawOrderbookMessage, rawBufferSize),
		FlattenedChan:  make(chan models.FlattenedOrderbookBatch, flattenedBufferSize),
		SortedChan:     make(chan models.SortedOrderbookBatch, sortedBufferSize),
		log:            log,
	}

	log.WithComponent("channels").WithFields(logger.Fields{
		"raw_buffer_size":       rawBufferSize,
		"flattened_buffer_size": flattenedBufferSize,
		"sorted_buffer_size":    sortedBufferSize,
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

func (c *Channels) logChannelStats(log *logger.Logger) {
	c.statsMutex.RLock()
	stats := c.stats
	c.statsMutex.RUnlock()

	log.WithComponent("channels").WithFields(logger.Fields{
		"raw_messages_sent":         stats.RawMessagesSent,
		"flattened_batches_sent":    stats.FlattenedBatchesSent,
		"sorted_batches_sent":       stats.SortedBatchesSent,
		"raw_messages_dropped":      stats.RawMessagesDropped,
		"flattened_batches_dropped": stats.FlattenedBatchesDropped,
		"sorted_batches_dropped":    stats.SortedBatchesDropped,
		"raw_channel_len":           len(c.RawMessageChan),
		"raw_channel_cap":           cap(c.RawMessageChan),
		"flattened_channel_len":     len(c.FlattenedChan),
		"flattened_channel_cap":     cap(c.FlattenedChan),
		"sorted_channel_len":        len(c.SortedChan),
		"sorted_channel_cap":        cap(c.SortedChan),
	}).Info("channel statistics")
}

func (c *Channels) Close() {
	if c.metricsReportTicker != nil {
		c.metricsReportTicker.Stop()
	}

	close(c.RawMessageChan)
	close(c.FlattenedChan)
	close(c.SortedChan)

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

func (c *Channels) IncrementSortedBatchesSent() {
	c.statsMutex.Lock()
	c.stats.SortedBatchesSent++
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

func (c *Channels) IncrementSortedBatchesDropped() {
	c.statsMutex.Lock()
	c.stats.SortedBatchesDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

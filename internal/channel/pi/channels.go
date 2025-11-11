package pi

import (
	"context"
	"sync"

	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

type ChannelStats struct {
	RawSent    int64
	RawDropped int64
}

type Channels struct {
	Raw chan models.RawPIMessage

	stats ChannelStats
	mu    sync.RWMutex
	log   *logger.Log
}

func NewChannels(rawBufferSize int) *Channels {
	log := logger.GetLogger()
	ch := &Channels{
		Raw: make(chan models.RawPIMessage, rawBufferSize),
		log: log,
	}

	log.WithComponent("pi_channels").WithFields(logger.Fields{
		"raw_buffer_size": rawBufferSize,
	}).Info("premium-index channels initialized")

	return ch
}

func (c *Channels) Close() {
	close(c.Raw)
	c.log.WithComponent("pi_channels").Info("premium-index channels closed")
}

func (c *Channels) SendRaw(ctx context.Context, msg models.RawPIMessage) bool {
	select {
	case c.Raw <- msg:
		c.incrementSent()
		return true
	case <-ctx.Done():
		return false
	default:
		c.incrementDropped()
		return false
	}
}

func (c *Channels) GetStats() ChannelStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *Channels) incrementSent() {
	c.mu.Lock()
	c.stats.RawSent++
	c.mu.Unlock()
}

func (c *Channels) incrementDropped() {
	c.mu.Lock()
	c.stats.RawDropped++
	c.mu.Unlock()
}

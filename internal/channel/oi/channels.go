package oi

import (
	"context"
	"sync"

	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// ChannelStats keeps counters for telemetry.
type ChannelStats struct {
	RawSent    int64
	RawDropped int64
}

// Channels exposes the raw open-interest stream.
type Channels struct {
	Raw chan models.RawOIMessage

	stats ChannelStats
	mu    sync.RWMutex
	log   *logger.Log
}

// NewChannels constructs the open interest channel group.
func NewChannels(rawBufferSize int) *Channels {
	log := logger.GetLogger()
	ch := &Channels{
		Raw: make(chan models.RawOIMessage, rawBufferSize),
		log: log,
	}

	log.WithComponent("oi_channels").WithFields(logger.Fields{
		"raw_buffer_size": rawBufferSize,
	}).Info("open-interest channels initialized")

	return ch
}

// Close releases all resources.
func (c *Channels) Close() {
	close(c.Raw)
	c.log.WithComponent("oi_channels").Info("open-interest channels closed")
}

// SendRaw attempts to enqueue a message on the raw channel.
func (c *Channels) SendRaw(ctx context.Context, msg models.RawOIMessage) bool {
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

// GetStats returns a snapshot of the channel counters.
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

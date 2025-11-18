package fpi

import (
	"context"
	"sync"

	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// ChannelStats keeps telemetry counters for the premium-index stream.
type ChannelStats struct {
	RawSent     int64
	NormSent    int64
	RawDropped  int64
	NormDropped int64
}

// Channels exposes fpi.raw and fpi.norm queues.
type Channels struct {
	Raw  chan models.RawFPI
	Norm chan models.NormFPI

	stats ChannelStats
	mu    sync.RWMutex
	log   *logger.Log
}

// NewChannels allocates buffered Raw/Norm premium-index channels.
func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	log := logger.GetLogger()
	ch := &Channels{
		Raw:  make(chan models.RawFPI, rawBufferSize),
		Norm: make(chan models.NormFPI, normBufferSize),
		log:  log,
	}

	log.WithComponent("fpi_channels").WithFields(logger.Fields{
		"raw_buffer_size":  rawBufferSize,
		"norm_buffer_size": normBufferSize,
	}).Info("FPI channels initialized")

	return ch
}

// Close terminates both channels.
func (c *Channels) Close() {
	close(c.Raw)
	close(c.Norm)
	c.log.WithComponent("fpi_channels").Info("FPI channels closed")
}

// SendRaw publishes a RawFPI into the buffered channel.
func (c *Channels) SendRaw(ctx context.Context, msg models.RawFPI) bool {
	select {
	case c.Raw <- msg:
		c.incrementRawSent()
		return true
	case <-ctx.Done():
		return false
	default:
		c.incrementRawDropped()
		return false
	}
}

// SendNorm publishes a normalized premium-index entry.
func (c *Channels) SendNorm(ctx context.Context, msg models.NormFPI) bool {
	select {
	case c.Norm <- msg:
		c.incrementNormSent()
		return true
	case <-ctx.Done():
		return false
	default:
		c.incrementNormDropped()
		return false
	}
}

// GetStats reports the accumulated counters.
func (c *Channels) GetStats() ChannelStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *Channels) incrementRawSent() {
	c.mu.Lock()
	c.stats.RawSent++
	c.mu.Unlock()
}

func (c *Channels) incrementNormSent() {
	c.mu.Lock()
	c.stats.NormSent++
	c.mu.Unlock()
}

func (c *Channels) incrementRawDropped() {
	c.mu.Lock()
	c.stats.RawDropped++
	c.mu.Unlock()
}

func (c *Channels) incrementNormDropped() {
	c.mu.Lock()
	c.stats.NormDropped++
	c.mu.Unlock()
}

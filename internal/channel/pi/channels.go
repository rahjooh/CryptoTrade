package pi

import (
	"context"
	"sync"

	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// ChannelStats tracks enqueue/dropped counters.
type ChannelStats struct {
	RawSent     int64
	NormSent    int64
	RawDropped  int64
	NormDropped int64
}

// Channels exposes raw and normalized premium-index streams.
type Channels struct {
	Raw  chan models.RawPIMessage
	Norm chan models.BatchPIMessage

	stats ChannelStats
	mu    sync.RWMutex
	log   *logger.Log
}

// NewChannels allocates buffered channels for premium-index ingestion.
func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	log := logger.GetLogger()
	ch := &Channels{
		Raw:  make(chan models.RawPIMessage, rawBufferSize),
		Norm: make(chan models.BatchPIMessage, normBufferSize),
		log:  log,
	}

	log.WithComponent("pi_channels").WithFields(logger.Fields{
		"raw_buffer_size":  rawBufferSize,
		"norm_buffer_size": normBufferSize,
	}).Info("premium-index channels initialized")

	return ch
}

// Close closes both raw and normalized channels.
func (c *Channels) Close() {
	close(c.Raw)
	close(c.Norm)
	c.log.WithComponent("pi_channels").Info("premium-index channels closed")
}

// SendRaw enqueues a raw premium-index payload.
func (c *Channels) SendRaw(ctx context.Context, msg models.RawPIMessage) bool {
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

// SendNorm enqueues a normalized batch for downstream consumers.
func (c *Channels) SendNorm(ctx context.Context, msg models.BatchPIMessage) bool {
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

// GetStats returns a snapshot of the telemetry counters.
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

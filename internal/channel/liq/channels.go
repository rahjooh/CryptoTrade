package liq

import (
	"context"
	"sync"

	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// ChannelStats exposes enqueue metrics for telemetry.
type ChannelStats struct {
	RawSent     int64
	NormSent    int64
	RawDropped  int64
	NormDropped int64
}

// Channels exposes raw and normalized liquidation streams.
type Channels struct {
	Raw  chan models.RawLiquidationMessage
	Norm chan models.BatchLiquidationMessage

	stats      ChannelStats
	statsMutex sync.RWMutex
	log        *logger.Log
}

// NewChannels constructs buffered channels for liquidation ingestion.
func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	log := logger.GetLogger()
	c := &Channels{
		Raw:  make(chan models.RawLiquidationMessage, rawBufferSize),
		Norm: make(chan models.BatchLiquidationMessage, normBufferSize),
		log:  log,
	}

	log.WithComponent("liq_channels").WithFields(logger.Fields{
		"raw_buffer_size":  rawBufferSize,
		"norm_buffer_size": normBufferSize,
	}).Info("liquidation channels initialized")

	return c
}

// Close shuts down both raw and normalized channels.
func (c *Channels) Close() {
	close(c.Raw)
	close(c.Norm)
	c.log.WithComponent("liq_channels").Info("liquidation channels closed")
}

func (c *Channels) incrementRawSent() {
	c.statsMutex.Lock()
	c.stats.RawSent++
	c.statsMutex.Unlock()
}

func (c *Channels) incrementNormSent() {
	c.statsMutex.Lock()
	c.stats.NormSent++
	c.statsMutex.Unlock()
}

func (c *Channels) incrementRawDropped() {
	c.statsMutex.Lock()
	c.stats.RawDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) incrementNormDropped() {
	c.statsMutex.Lock()
	c.stats.NormDropped++
	c.statsMutex.Unlock()
}

// SendRaw enqueues a raw liquidation payload if capacity is available.
func (c *Channels) SendRaw(ctx context.Context, msg models.RawLiquidationMessage) bool {
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

// SendNorm enqueues a normalized batch for downstream writers.
func (c *Channels) SendNorm(ctx context.Context, msg models.BatchLiquidationMessage) bool {
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

// GetStats returns aggregated counters for monitoring.
func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

package fobs

import (
	"context"
	"sync"

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

	stats      ChannelStats
	statsMutex sync.RWMutex
	log        *logger.Log
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

func (c *Channels) Close() {
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
}

func (c *Channels) IncrementNormDropped() {
	c.statsMutex.Lock()
	c.stats.NormDropped++
	c.statsMutex.Unlock()
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

package fobd

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
	Raw  chan models.RawFOBDMessage
	Norm chan models.BatchFOBDMessage

	stats      ChannelStats
	statsMutex sync.RWMutex
	log        *logger.Log
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	log := logger.GetLogger()
	c := &Channels{
		Raw:  make(chan models.RawFOBDMessage, rawBufferSize),
		Norm: make(chan models.BatchFOBDMessage, normBufferSize),
		log:  log,
	}

	log.WithComponent("fobd_channels").WithFields(logger.Fields{
		"raw_buffer_size":  rawBufferSize,
		"norm_buffer_size": normBufferSize,
	}).Info("FOBD channels initialized")

	return c
}

func (c *Channels) Close() {
	close(c.Raw)
	close(c.Norm)
	c.log.WithComponent("fobd_channels").Info("FOBD channels closed")
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

func (c *Channels) SendRaw(ctx context.Context, msg models.RawFOBDMessage) bool {
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

func (c *Channels) SendNorm(ctx context.Context, msg models.BatchFOBDMessage) bool {
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

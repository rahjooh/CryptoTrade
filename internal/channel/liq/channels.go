package liq

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
	Raw chan models.RawLiquidationMessage

	stats      ChannelStats
	statsMutex sync.RWMutex
	log        *logger.Log
}

func NewChannels(rawBufferSize int) *Channels {
	log := logger.GetLogger()
	c := &Channels{
		Raw: make(chan models.RawLiquidationMessage, rawBufferSize),
		log: log,
	}

	log.WithComponent("liq_channels").WithFields(logger.Fields{
		"raw_buffer_size": rawBufferSize,
	}).Info("liquidation channels initialized")

	return c
}

func (c *Channels) Close() {
	close(c.Raw)
	c.log.WithComponent("liq_channels").Info("liquidation channels closed")
}

func (c *Channels) IncrementRawSent() {
	c.statsMutex.Lock()
	c.stats.RawSent++
	c.statsMutex.Unlock()
}

func (c *Channels) IncrementRawDropped() {
	c.statsMutex.Lock()
	c.stats.RawDropped++
	c.statsMutex.Unlock()
}

func (c *Channels) SendRaw(ctx context.Context, msg models.RawLiquidationMessage) bool {
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

func (c *Channels) GetStats() ChannelStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

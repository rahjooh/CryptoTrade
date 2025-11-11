package channel

import (
	"cryptoflow/internal/channel/fobd"
	"cryptoflow/internal/channel/fobs"
	"cryptoflow/internal/channel/liq"
	"cryptoflow/internal/channel/oi"
)

type Channels struct {
	FOBS *fobs.Channels
	FOBD *fobd.Channels
	Liq  *liq.Channels
	OI   *oi.Channels
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	return &Channels{
		FOBS: fobs.NewChannels(rawBufferSize, normBufferSize),
		FOBD: fobd.NewChannels(rawBufferSize, normBufferSize),
		Liq:  liq.NewChannels(rawBufferSize),
		OI:   oi.NewChannels(rawBufferSize),
	}
}

func (c *Channels) Close() {
	if c.FOBS != nil {
		c.FOBS.Close()
	}
	if c.FOBD != nil {
		c.FOBD.Close()
	}
	if c.Liq != nil {
		c.Liq.Close()
	}
	if c.OI != nil {
		c.OI.Close()
	}
}

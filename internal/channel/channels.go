package channel

import (
	"cryptoflow/internal/channel/fobd"
	"cryptoflow/internal/channel/fobs"
	"cryptoflow/internal/channel/liq"
)

type Channels struct {
	FOBS *fobs.Channels
	FOBD *fobd.Channels
	Liq  *liq.Channels
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	return &Channels{
		FOBS: fobs.NewChannels(rawBufferSize, normBufferSize),
		FOBD: fobd.NewChannels(rawBufferSize, normBufferSize),
		Liq:  liq.NewChannels(rawBufferSize),
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
}

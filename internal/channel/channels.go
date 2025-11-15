package channel

import (
	"cryptoflow/internal/channel/fobd"
	"cryptoflow/internal/channel/fobs"
	"cryptoflow/internal/channel/foi"
	"cryptoflow/internal/channel/liq"
	"cryptoflow/internal/channel/pi"
)

type Channels struct {
	FOBS *fobs.Channels
	FOBD *fobd.Channels
	FOI  *foi.Channels
	Liq  *liq.Channels
	PI   *pi.Channels
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	return &Channels{
		FOBS: fobs.NewChannels(rawBufferSize, normBufferSize),
		FOBD: fobd.NewChannels(rawBufferSize, normBufferSize),
		FOI:  foi.NewChannels(rawBufferSize, normBufferSize),
		Liq:  liq.NewChannels(rawBufferSize, normBufferSize),
		PI:   pi.NewChannels(rawBufferSize, normBufferSize),
	}
}

func (c *Channels) Close() {
	if c.FOBS != nil {
		c.FOBS.Close()
	}
	if c.FOBD != nil {
		c.FOBD.Close()
	}
	if c.FOI != nil {
		c.FOI.Close()
	}
	if c.Liq != nil {
		c.Liq.Close()
	}
	if c.PI != nil {
		c.PI.Close()
	}
}

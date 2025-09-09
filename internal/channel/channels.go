package channel

import (
	"context"

	"cryptoflow/internal/channel/fobd"
	"cryptoflow/internal/channel/fobs"
)

type Channels struct {
	FOBS *fobs.Channels
	FOBD *fobd.Channels
}

func NewChannels(rawBufferSize, normBufferSize int) *Channels {
	return &Channels{
		FOBS: fobs.NewChannels(rawBufferSize, normBufferSize),
		FOBD: fobd.NewChannels(rawBufferSize, normBufferSize),
	}
}

func (c *Channels) StartMetricsReporting(ctx context.Context) {
	if c.FOBS != nil {
		c.FOBS.StartMetricsReporting(ctx)
	}
	if c.FOBD != nil {
		c.FOBD.StartMetricsReporting(ctx)
	}
}

func (c *Channels) Close() {
	if c.FOBS != nil {
		c.FOBS.Close()
	}
	if c.FOBD != nil {
		c.FOBD.Close()
	}
}

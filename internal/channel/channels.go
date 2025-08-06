package pipeline

import (
	"CryptoFlow/internal/config"
)

// PipelineChannels encapsulates all communication channels used across the system.
type PipelineChannels struct {
	HighFreqCh  chan config.MarketData // e.g., deltas
	LowFreqCh   chan config.MarketData // e.g., snapshots
	MetadataCh  chan config.MarketData // e.g., contracts, funding, etc.
	HighBatchCh chan []config.MarketData
	LowBatchCh  chan []config.MarketData
	MetaBatchCh chan []config.MarketData
}

// NewPipelineChannels initializes all required channels with best-practice buffer sizes.
func NewPipelineChannels() *PipelineChannels {
	return &PipelineChannels{
		HighFreqCh:  make(chan config.MarketData, 15000),
		LowFreqCh:   make(chan config.MarketData, 5000),
		MetadataCh:  make(chan config.MarketData, 1000),
		HighBatchCh: make(chan []config.MarketData, 1000),
		LowBatchCh:  make(chan []config.MarketData, 500),
		MetaBatchCh: make(chan []config.MarketData, 100),
	}
}

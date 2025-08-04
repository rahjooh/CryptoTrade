package channel

import "CryptoFlow/internal/model"

type ChannelManager struct {
	spotSnapshotChan chan *model.SpotOrderBookSnapshot
}

func NewChannelManager(bufferSize int) *ChannelManager {
	return &ChannelManager{
		spotSnapshotChan: make(chan *model.SpotOrderBookSnapshot, bufferSize),
	}
}

func (m *ChannelManager) SpotSnapshotWriter() chan<- *model.SpotOrderBookSnapshot {
	return m.spotSnapshotChan
}

func (m *ChannelManager) SpotSnapshotReader() <-chan *model.SpotOrderBookSnapshot {
	return m.spotSnapshotChan
}

func (m *ChannelManager) Close() {
	close(m.spotSnapshotChan)
}

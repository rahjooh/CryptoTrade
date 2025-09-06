package writer

import (
	"testing"

	"cryptoflow/logger"
	"cryptoflow/models"
)

func TestAddBatchAndBufferKey(t *testing.T) {
	w := &SnapshotWriter{
		log:    logger.GetLogger(),
		buffer: make(map[string][]models.NormFOBSMessage),
	}
	batch := models.BatchFOBSMessage{
		Exchange:    "binance",
		Market:      "future",
		Symbol:      "BTCUSDT",
		Entries:     []models.NormFOBSMessage{{Symbol: "BTCUSDT"}},
		RecordCount: 1,
	}
	w.addBatch(batch)
	key := w.bufferKey("binance", "future", "BTCUSDT")
	entries, ok := w.buffer[key]
	if !ok || len(entries) != 1 {
		t.Fatalf("expected batch to be added, got %v", w.buffer)
	}
}

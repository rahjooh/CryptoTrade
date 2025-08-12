package metadata

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGeneratorCreatesMetadata(t *testing.T) {
	dir := t.TempDir()
	gen := NewGenerator(dir, "s3://bucket", "bucket", "", "orderbook", "", nil, nil)
	df := DataFile{
		Path:        "s3://bucket/exchange=binance/symbol=BTCUSDT/year=2025/month=08/day=11/hour=06/file.parquet",
		FileSize:    100,
		RecordCount: 10,
		Partition: map[string]any{
			"exchange": "binance",
			"symbol":   "BTCUSDT",
			"year":     2025,
			"month":    8,
			"day":      11,
			"hour":     6,
		},
		Timestamp: time.Unix(0, 0),
	}
	if err := gen.AddFile(df); err != nil {
		t.Fatalf("AddFile: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "metadata", "metadata.json")); err != nil {
		t.Fatalf("metadata not written: %v", err)
	}
	catalogDir := filepath.Join(dir, "catalog")
	if err := gen.WriteCatalogEntry(catalogDir); err != nil {
		t.Fatalf("catalog entry: %v", err)
	}
	if _, err := os.Stat(filepath.Join(catalogDir, "orderbook.json")); err != nil {
		t.Fatalf("catalog entry not written: %v", err)
	}
}

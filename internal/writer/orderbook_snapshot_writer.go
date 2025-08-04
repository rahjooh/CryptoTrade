// Maintains one Parquet writer per table (bids, asks) per hour per symbol
// Rotates writers when hour changes
// Uses mutexes to safely write concurrently
package writer

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"path/filepath"
	"sync"
	"time"

	"CryptoFlow/internal/config"
	"CryptoFlow/internal/model"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetWriter struct {
	pw *writer.ParquetWriter
	fw source.ParquetFile
	mu sync.Mutex
}

func (w *ParquetWriter) Write(row *model.SpotOrderBookSnapshotToParquet) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pw.Write(row)
}

func (w *ParquetWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.pw.WriteStop(); err != nil {
		return err
	}
	return w.fw.Close()
}

type WriterPool struct {
	mu      sync.Mutex
	writers map[string]*ParquetWriter
}

func NewWriterPool() *WriterPool {
	return &WriterPool{writers: make(map[string]*ParquetWriter)}
}

func (p *WriterPool) getFilePath(symbol string, table string, ts time.Time) string {
	return filepath.Join(config.DataDir, symbol, table, ts.Format("2006-01-02T15")+".parquet")
}

func (p *WriterPool) GetWriter(symbol string, table string, ts time.Time) (*ParquetWriter, error) {
	path := p.getFilePath(symbol, table, ts)
	p.mu.Lock()
	defer p.mu.Unlock()

	if w, ok := p.writers[path]; ok {
		return w, nil
	}

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return nil, err
	}
	pw, err := writer.NewParquetWriter(fw, new(model.SpotOrderBookSnapshotToParquet), 1)
	if err != nil {
		return nil, err
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	w := &ParquetWriter{pw: pw, fw: fw}
	p.writers[path] = w
	return w, nil
}

func (p *WriterPool) CloseAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.writers {
		_ = w.Close()
	}
	return nil
}

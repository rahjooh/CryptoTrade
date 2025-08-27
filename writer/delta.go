package writer

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	appconfig "cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// deltaRecord defines parquet schema for order book delta entries
// Level information is not included as deltas contain only price updates
// and associated quantities.
type deltaRecord struct {
	Symbol        string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventTime     int64   `parquet:"name=event_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	UpdateID      int64   `parquet:"name=update_id, type=INT64"`
	PrevUpdateID  int64   `parquet:"name=prev_update_id, type=INT64"`
	FirstUpdateID int64   `parquet:"name=first_update_id, type=INT64"`
	Side          string  `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8"`
	Price         float64 `parquet:"name=price, type=DOUBLE"`
	Quantity      float64 `parquet:"name=quantity, type=DOUBLE"`
	ReceivedTime  int64   `parquet:"name=received_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
}

// memory file writer reused from s3_writer.go
type memFileWriter struct{ buffer *bytes.Buffer }

func newMemFileWriter() *memFileWriter { return &memFileWriter{buffer: &bytes.Buffer{}} }

func (m *memFileWriter) Create(string) (source.ParquetFile, error) { return m, nil }
func (m *memFileWriter) Open(string) (source.ParquetFile, error)   { return m, nil }
func (m *memFileWriter) Seek(int64, int) (int64, error)            { return int64(m.buffer.Len()), nil }
func (m *memFileWriter) Read([]byte) (int, error)                  { return 0, nil }
func (m *memFileWriter) Write(b []byte) (int, error)               { return m.buffer.Write(b) }
func (m *memFileWriter) Close() error                              { return nil }
func (m *memFileWriter) Bytes() []byte                             { return m.buffer.Bytes() }

// DeltaWriter consumes normalized delta batches and writes them to S3 in
// parquet format. Data is buffered per symbol and flushed periodically or
// when buffer size exceeds processor batch size.
type DeltaWriter struct {
	cfg         *appconfig.Config
	normChan    <-chan models.OrderbookDeltaBatch
	s3Client    *s3.Client
	buffer      map[string][]models.OrderbookDeltaEntry
	mu          sync.Mutex
	flushTicker *time.Ticker
	ctx         context.Context
	wg          *sync.WaitGroup
	running     bool
	log         *logger.Log
}

// NewDeltaWriter initializes a delta writer with AWS credentials.
func NewDeltaWriter(cfg *appconfig.Config, normChan <-chan models.OrderbookDeltaBatch) (*DeltaWriter, error) {
	log := logger.GetLogger()
	ctx := context.Background()
	loadOpts := []func(*config.LoadOptions) error{config.WithRegion(cfg.Storage.S3.Region)}
	if cfg.Storage.S3.AccessKeyID != "" && cfg.Storage.S3.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.Storage.S3.AccessKeyID,
				cfg.Storage.S3.SecretAccessKey,
				"",
			)))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Storage.S3.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Storage.S3.Endpoint)
		}
		o.UsePathStyle = cfg.Storage.S3.PathStyle
	})
	return &DeltaWriter{
		cfg:      cfg,
		normChan: normChan,
		s3Client: s3Client,
		buffer:   make(map[string][]models.OrderbookDeltaEntry),
		wg:       &sync.WaitGroup{},
		log:      log,
	}, nil
}

// Start launches workers and flush ticker.
func (w *DeltaWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("delta writer already running")
	}
	w.running = true
	w.ctx = ctx
	w.flushTicker = time.NewTicker(w.cfg.Storage.S3.FlushInterval)
	w.mu.Unlock()

	w.wg.Add(1)
	go w.worker()

	w.wg.Add(1)
	go w.flushLoop()

	w.log.WithComponent("delta_writer").Info("delta writer started")
	return nil
}

// Stop waits for workers and flushes remaining data.
func (w *DeltaWriter) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	w.mu.Unlock()

	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	w.wg.Wait()
	w.flushAll()
	w.log.WithComponent("delta_writer").Info("delta writer stopped")
}

func (w *DeltaWriter) worker() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case batch, ok := <-w.normChan:
			if !ok {
				return
			}
			key := fmt.Sprintf("%s|%s|%s", batch.Exchange, batch.Market, batch.Symbol)
			w.mu.Lock()
			w.buffer[key] = append(w.buffer[key], batch.Entries...)
			shouldFlush := len(w.buffer[key]) >= w.cfg.Processor.BatchSize
			w.mu.Unlock()
			if shouldFlush {
				w.flushBuffer(key)
			}
		}
	}
}

func (w *DeltaWriter) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.flushTicker.C:
			w.flushAll()
		}
	}
}

func (w *DeltaWriter) flushAll() {
	w.mu.Lock()
	keys := make([]string, 0, len(w.buffer))
	for k := range w.buffer {
		keys = append(keys, k)
	}
	w.mu.Unlock()
	for _, k := range keys {
		w.flushBuffer(k)
	}
}

func (w *DeltaWriter) flushBuffer(key string) {
	w.mu.Lock()
	entries := w.buffer[key]
	if len(entries) == 0 {
		w.mu.Unlock()
		return
	}
	delete(w.buffer, key)
	w.mu.Unlock()

	parts := strings.SplitN(key, "|", 3)
	batch := models.OrderbookDeltaBatch{
		BatchID:     uuid.New().String(),
		Exchange:    parts[0],
		Market:      parts[1],
		Symbol:      parts[2],
		Entries:     entries,
		RecordCount: len(entries),
		Timestamp:   time.Now(),
	}
	w.writeBatch(batch)
}

func (w *DeltaWriter) writeBatch(batch models.OrderbookDeltaBatch) {
	data, size, err := w.createParquet(batch.Entries)
	if err != nil {
		w.log.WithComponent("delta_writer").WithError(err).Error("create parquet failed")
		return
	}
	key := w.s3Key(batch)
	if err := w.upload(key, data); err != nil {
		w.log.WithComponent("delta_writer").WithError(err).Error("upload to s3 failed")
		return
	}
	w.log.WithComponent("delta_writer").WithFields(logger.Fields{"s3_key": key, "records": batch.RecordCount, "bytes": size}).Info("delta batch uploaded")
}

func (w *DeltaWriter) createParquet(entries []models.OrderbookDeltaEntry) ([]byte, int64, error) {
	mw := newMemFileWriter()
	pw, err := writer.NewParquetWriter(mw, new(deltaRecord), 4)
	if err != nil {
		return nil, 0, err
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for _, e := range entries {
		rec := deltaRecord{
			Symbol:        e.Symbol,
			EventTime:     e.EventTime,
			UpdateID:      e.UpdateID,
			PrevUpdateID:  e.PrevUpdateID,
			FirstUpdateID: e.FirstUpdateID,
			Side:          e.Side,
			Price:         e.Price,
			Quantity:      e.Quantity,
			ReceivedTime:  e.ReceivedTime,
		}
		if err := pw.Write(rec); err != nil {
			return nil, 0, err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return nil, 0, err
	}
	return mw.Bytes(), int64(len(mw.Bytes())), nil
}

func (w *DeltaWriter) upload(key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Storage.S3.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	_, err := w.s3Client.PutObject(w.ctx, input)
	return err
}

func (w *DeltaWriter) s3Key(batch models.OrderbookDeltaBatch) string {
	timestamp := batch.Timestamp
	parts := []string{
		fmt.Sprintf("exchange=%s", batch.Exchange),
		fmt.Sprintf("market=%s", batch.Market),
		fmt.Sprintf("symbol=%s", batch.Symbol),
		fmt.Sprintf("year=%04d", timestamp.Year()),
		fmt.Sprintf("month=%02d", int(timestamp.Month())),
		fmt.Sprintf("day=%02d", timestamp.Day()),
		fmt.Sprintf("hour=%02d", timestamp.Hour()),
	}
	filename := fmt.Sprintf("delta_%s_%s_%d.parquet", batch.Exchange, batch.Symbol, timestamp.UnixNano())
	return filepath.ToSlash(filepath.Join(append(parts, filename)...))
}

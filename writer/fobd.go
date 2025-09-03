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

// NewDeltaWriter initializes a delta writer with AWS credentials.
func NewDeltaWriter(cfg *appconfig.Config, normChan <-chan models.BatchFOBDMessage) (*DeltaWriter, error) {
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
		buffer:   make(map[string][]models.NormFOBDMessage),
		wg:       &sync.WaitGroup{},
		log:      log,
	}, nil
}

// memory file writer reused from fobs.go
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
// parquet format. Data is buffered per symbol and flushed periodically based
// on configured flush interval.
type DeltaWriter struct {
	cfg         *appconfig.Config
	normChan    <-chan models.BatchFOBDMessage
	s3Client    *s3.Client
	buffer      map[string][]models.NormFOBDMessage
	mu          sync.Mutex
	flushTicker *time.Ticker
	ctx         context.Context
	wg          *sync.WaitGroup
	running     bool
	log         *logger.Log
}

// Start launches workers and flush ticker.
func (w *DeltaWriter) start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("delta writer already running")
	}
	w.running = true
	w.ctx = ctx
	w.flushTicker = time.NewTicker(w.cfg.Writer.Buffer.DeltaFlushInterval)
	w.mu.Unlock()

	w.wg.Add(1)
	go w.worker()

	w.wg.Add(1)
	go w.flushLoop()

	w.wg.Add(1)
	go w.metricsReporter()

	w.log.WithComponent("delta_writer").Info("delta writer started")
	return nil
}

// Stop waits for workers and flushes remaining data.
func (w *DeltaWriter) stop() {
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
	w.flushBuffers()
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
			w.addBatch(batch)
		}
	}
}

func (w *DeltaWriter) addBatch(batch models.BatchFOBDMessage) {
	key := fmt.Sprintf("%s|%s|%s", batch.Exchange, batch.Market, batch.Symbol)
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], batch.Entries...)
	size := len(w.buffer[key])
	w.mu.Unlock()

	if w.cfg.Writer.Buffer.MaxSize > 0 && size >= w.cfg.Writer.Buffer.MaxSize {
		w.flushKey(key)
	}
}

func (w *DeltaWriter) flushKey(key string) {
	w.mu.Lock()
	entries, ok := w.buffer[key]
	if !ok || len(entries) == 0 {
		w.mu.Unlock()
		return
	}
	delete(w.buffer, key)
	w.mu.Unlock()

	parts := strings.SplitN(key, "|", 3)
	batch := models.BatchFOBDMessage{
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

func (w *DeltaWriter) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			w.flushBuffers()
			return
		case <-w.flushTicker.C:
			w.flushBuffers()
		}
	}
}

func (w *DeltaWriter) flushBuffers() {
	w.mu.Lock()
	buffers := w.buffer
	w.buffer = make(map[string][]models.NormFOBDMessage)
	w.mu.Unlock()

	if len(buffers) == 0 {
		return
	}

	for key, entries := range buffers {
		if len(entries) == 0 {
			continue
		}
		parts := strings.SplitN(key, "|", 3)
		batch := models.BatchFOBDMessage{
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
}

func (w *DeltaWriter) writeBatch(batch models.BatchFOBDMessage) {
	start := time.Now()
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
	duration := time.Since(start)
	fields := logger.Fields{
		"s3_key":      key,
		"records":     batch.RecordCount,
		"bytes":       size,
		"duration_ms": float64(duration.Nanoseconds()) / 1e6,
	}
	if duration > 0 {
		fields["throughput_bytes_per_sec"] = float64(size) / duration.Seconds()
	}
	w.log.WithComponent("delta_writer").WithFields(fields).Info("delta batch uploaded")
	logger.IncrementS3WriteDelta(size)
}

func (w *DeltaWriter) createParquet(entries []models.NormFOBDMessage) ([]byte, int64, error) {
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
	ctx := context.WithoutCancel(w.ctx)
	_, err := w.s3Client.PutObject(ctx, input)
	return err
}

func (w *DeltaWriter) metricsReporter() {
	defer w.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			running := w.running
			w.mu.Unlock()
			if !running {
				return
			}
			w.log.WithComponent("delta_writer").WithFields(logger.Fields{
				"norm_channel_len": len(w.normChan),
				"norm_channel_cap": cap(w.normChan),
			}).Info("delta writer channel size")
		}
	}
}

// Start exposes the internal start method for external packages.
func (w *DeltaWriter) Start(ctx context.Context) error { return w.start(ctx) }

// Stop exposes the internal stop method for external packages.
func (w *DeltaWriter) Stop() { w.stop() }

func (w *DeltaWriter) s3Key(batch models.BatchFOBDMessage) string {
	timestamp := batch.Timestamp

	var parts []string
	for _, k := range w.cfg.Writer.Partitioning.AdditionalKeys {
		switch k {
		case "exchange":
			parts = append(parts, fmt.Sprintf("exchange=%s", batch.Exchange))
		case "market":
			parts = append(parts, fmt.Sprintf("market=%s", batch.Market))
		case "symbol":
			parts = append(parts, fmt.Sprintf("symbol=%s", batch.Symbol))
		}
	}

	timePath := w.cfg.Writer.Partitioning.TimeFormat
	timePath = strings.ReplaceAll(timePath, "{year}", fmt.Sprintf("%04d", timestamp.Year()))
	timePath = strings.ReplaceAll(timePath, "{month}", fmt.Sprintf("%02d", int(timestamp.Month())))
	timePath = strings.ReplaceAll(timePath, "{day}", fmt.Sprintf("%02d", timestamp.Day()))
	timePath = strings.ReplaceAll(timePath, "{hour}", fmt.Sprintf("%02d", timestamp.Hour()))

	parts = append(parts, timePath)

	filename := fmt.Sprintf("delta_%s_%s_%d.parquet", batch.Exchange, batch.Symbol, timestamp.UnixNano())
	return filepath.ToSlash(filepath.Join(append(parts, filename)...))
}

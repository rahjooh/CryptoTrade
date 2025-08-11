package writer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/xitongsys/parquet-go/source"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/metadata"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// ParquetRecord represents the structure of our parquet file
type ParquetRecord struct {
	Exchange     string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol       string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp    int64   `parquet:"name=timestamp, type=INT64"`
	LastUpdateID int64   `parquet:"name=last_update_id, type=INT64"`
	Side         string  `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8"`
	Price        float64 `parquet:"name=price, type=DOUBLE"`
	Quantity     float64 `parquet:"name=quantity, type=DOUBLE"`
	Level        int32   `parquet:"name=level, type=INT32"`
}

// memoryFileWriter implements ParquetFile interface for in-memory writing
type memoryFileWriter struct {
	buffer *bytes.Buffer
}

func newMemoryFileWriter() *memoryFileWriter {
	return &memoryFileWriter{
		buffer: &bytes.Buffer{},
	}
}

func (mfw *memoryFileWriter) Create(name string) (source.ParquetFile, error) {
	return mfw, nil
}

func (mfw *memoryFileWriter) Open(name string) (source.ParquetFile, error) {
	return mfw, nil
}

func (mfw *memoryFileWriter) Seek(offset int64, whence int) (int64, error) {
	// For writing, we typically don't need seek functionality
	// This is a simplified implementation
	return int64(mfw.buffer.Len()), nil
}

func (mfw *memoryFileWriter) Read(b []byte) (int, error) {
	return mfw.buffer.Read(b)
}

func (mfw *memoryFileWriter) Write(b []byte) (int, error) {
	return mfw.buffer.Write(b)
}

func (mfw *memoryFileWriter) Close() error {
	return nil
}

func (mfw *memoryFileWriter) Bytes() []byte {
	return mfw.buffer.Bytes()
}

type S3Writer struct {
	config        *appconfig.Config
	flattenedChan <-chan models.FlattenedOrderbookBatch
	s3Client      *s3.Client
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
	buffer        map[string][]models.FlattenedOrderbookEntry
	flushTicker   *time.Ticker
	metaGen       *metadata.Generator

	// Metrics
	batchesWritten int64
	filesWritten   int64
	bytesWritten   int64
	errorsCount    int64
}

func (w *S3Writer) addBatch(batch models.FlattenedOrderbookBatch) {
	key := w.bufferKey(batch.Exchange, batch.Market, batch.Symbol)
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], batch.Entries...)
	w.mu.Unlock()
}

func (w *S3Writer) bufferKey(exchange, market, symbol string) string {
	return fmt.Sprintf("%s|%s|%s", exchange, market, symbol)
}

func (w *S3Writer) flushWorker() {
	defer w.wg.Done()

	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{"worker": "flush"})
	log.Info("starting flush worker")

	for {
		select {
		case <-w.ctx.Done():
			w.flushBuffers("shutdown")
			log.Info("flush worker stopped due to context cancellation")
			return
		case <-w.flushTicker.C:
			w.flushBuffers("interval")
		}
	}
}

func (w *S3Writer) flushBuffers(reason string) {
	w.mu.Lock()
	buffers := w.buffer
	w.buffer = make(map[string][]models.FlattenedOrderbookEntry)
	w.mu.Unlock()

	if len(buffers) == 0 {
		return
	}

	w.log.WithComponent("s3_writer").WithFields(logger.Fields{
		"flushed_buffers": len(buffers),
		"reason":          reason,
	}).Info("flushing buffers")

	for key, entries := range buffers {
		if len(entries) == 0 {
			continue
		}
		parts := strings.SplitN(key, "|", 3)
		batch := models.FlattenedOrderbookBatch{
			BatchID:     uuid.New().String(),
			Exchange:    parts[0],
			Market:      parts[1],
			Symbol:      parts[2],
			Entries:     entries,
			RecordCount: len(entries),
			Timestamp:   time.Now(),
		}
		w.processBatch(batch)
	}
}
func NewS3Writer(cfg *appconfig.Config, flattenedChan <-chan models.FlattenedOrderbookBatch) (*S3Writer, error) {
	log := logger.GetLogger()

	ctx := context.Background()

	// Configure AWS options
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Storage.S3.Region),
	}
	if cfg.Storage.S3.AccessKeyID != "" && cfg.Storage.S3.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.Storage.S3.AccessKeyID,
				cfg.Storage.S3.SecretAccessKey,
				"",
			),
		))
	}

	awsConfig, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		log.WithComponent("s3_writer").WithError(err).Warn("failed to load AWS configuration")
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Validate credentials
	creds, err := awsConfig.Credentials.Retrieve(ctx)
	if err != nil || !creds.HasKeys() {
		return nil, fmt.Errorf("aws credentials not found")
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		if cfg.Storage.S3.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Storage.S3.Endpoint)
		}
		o.UsePathStyle = cfg.Storage.S3.PathStyle
	})

	metaDir, err := os.MkdirTemp("", "iceberg")
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	gen := metadata.NewGenerator(metaDir, fmt.Sprintf("s3://%s", cfg.Storage.S3.Bucket), cfg.Storage.S3.Bucket, "", cfg.Cryptoflow.Name, s3Client)

	s3Writer := &S3Writer{
		config:        cfg,
		flattenedChan: flattenedChan,
		s3Client:      s3Client,
		wg:            &sync.WaitGroup{},
		log:           log,
		metaGen:       gen,
	}

	log.WithComponent("s3_writer").WithFields(logger.Fields{
		"bucket":     cfg.Storage.S3.Bucket,
		"region":     cfg.Storage.S3.Region,
		"endpoint":   cfg.Storage.S3.Endpoint,
		"path_style": cfg.Storage.S3.PathStyle,
	}).Info("s3 writer initialized")

	return s3Writer, nil
}

func (w *S3Writer) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("s3 writer already running")
	}
	w.running = true
	w.ctx = ctx
	w.mu.Unlock()

	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting s3 writer")

	w.buffer = make(map[string][]models.FlattenedOrderbookEntry)
	w.flushTicker = time.NewTicker(w.config.Storage.S3.FlushInterval)

	// Start multiple workers for parallel processing
	numWorkers := w.config.Writer.MaxWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}

	log.WithFields(logger.Fields{"workers": numWorkers}).Info("starting s3 writer workers")

	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}

	w.wg.Add(1)
	go w.flushWorker()

	// Start metrics reporter
	go w.metricsReporter(ctx)

	log.Info("s3 writer started successfully")
	return nil
}

func (w *S3Writer) Stop() {
	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}

	w.log.WithComponent("s3_writer").Info("stopping s3 writer")
	w.wg.Wait()
	w.log.WithComponent("s3_writer").Info("s3 writer stopped")
}

func (w *S3Writer) worker(workerID int) {
	defer w.wg.Done()

	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{
		"worker_id": workerID,
		"worker":    "s3_writer",
	})

	log.Info("starting s3 writer worker")

	for {
		select {
		case <-w.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case batch, ok := <-w.flattenedChan:
			if !ok {
				log.Info("flattened channel closed, worker stopping")
				return
			}
			w.addBatch(batch)
			atomic.AddInt64(&w.batchesWritten, 1)
			logger.LogDataFlowEntry(log, "flattened_channel", "s3", batch.RecordCount, "parquet_file")
		}
	}
}

func (w *S3Writer) processBatch(batch models.FlattenedOrderbookBatch) {
	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{
		"batch_id":     batch.BatchID,
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"record_count": batch.RecordCount,
		"timestamp":    batch.Timestamp,
		"operation":    "process_batch",
	})

	log.Info("processing batch")

	if batch.RecordCount == 0 {
		log.Debug("batch has no records, skipping")
		return
	}

	// Generate S3 key path
	s3Key := w.generateS3Key(batch)
	log = log.WithFields(logger.Fields{"s3_key": s3Key})

	// Create parquet file in memory
	parquetData, fileSize, err := w.createParquetFile(batch.Entries)
	if err != nil {
		atomic.AddInt64(&w.errorsCount, 1)
		log.WithError(err).Error("failed to create parquet file")
		return
	}

	// Upload to S3
	err = w.uploadToS3(s3Key, parquetData)
	if err != nil {
		atomic.AddInt64(&w.errorsCount, 1)
		log.WithError(err).
			WithEnv("S3_BUCKET").
			WithFields(logger.Fields{"bucket": w.config.Storage.S3.Bucket, "s3_key": s3Key}).
			Error("failed to upload to S3")
		return
	}

	// Update metrics
	atomic.AddInt64(&w.filesWritten, 1)
	atomic.AddInt64(&w.bytesWritten, fileSize)

	log.WithFields(logger.Fields{
		"file_size": fileSize,
	}).Info("batch processed and uploaded successfully")

	logger.LogDataFlowEntry(log, "flattened_channel", "s3", batch.RecordCount, "parquet_file")
	w.log.LogMetric("s3_writer", "file_uploaded", 1, "counter", logger.Fields{
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"file_size":    fileSize,
		"record_count": batch.RecordCount,
	})

	df := metadata.DataFile{
		Path:        fmt.Sprintf("s3://%s/%s", w.config.Storage.S3.Bucket, s3Key),
		FileSize:    fileSize,
		RecordCount: int64(batch.RecordCount),
		Partition: map[string]any{
			"exchange": batch.Exchange,
			"market":   batch.Market,
			"symbol":   batch.Symbol,
			"year":     batch.Timestamp.Year(),
			"month":    int(batch.Timestamp.Month()),
			"day":      batch.Timestamp.Day(),
			"hour":     batch.Timestamp.Hour(),
		},
		Timestamp: batch.Timestamp,
	}
	if err := w.metaGen.AddFile(df); err != nil {
		log.WithError(err).Warn("failed to update metadata")
	}
}

func (w *S3Writer) generateS3Key(batch models.FlattenedOrderbookBatch) string {
	timestamp := batch.Timestamp

	// Build key parts from additional keys in order
	var parts []string
	for _, k := range w.config.Writer.Partitioning.AdditionalKeys {
		switch k {
		case "exchange":
			parts = append(parts, fmt.Sprintf("exchange=%s", batch.Exchange))
		case "symbol":
			parts = append(parts, fmt.Sprintf("symbol=%s", batch.Symbol))
		case "market":
			if batch.Market != "" {
				parts = append(parts, fmt.Sprintf("market=%s", batch.Market))
			}
		}
	}

	// Time-based partition path
	timeFormat := w.config.Writer.Partitioning.TimeFormat
	timePath := strings.ReplaceAll(timeFormat, "{year}", fmt.Sprintf("%04d", timestamp.Year()))
	timePath = strings.ReplaceAll(timePath, "{month}", fmt.Sprintf("%02d", timestamp.Month()))
	timePath = strings.ReplaceAll(timePath, "{day}", fmt.Sprintf("%02d", timestamp.Day()))
	timePath = strings.ReplaceAll(timePath, "{hour}", fmt.Sprintf("%02d", timestamp.Hour()))

	parts = append(parts, timePath)

	// Add filename
	filename := fmt.Sprintf("orderbook_%s_%s_%d.parquet",
		batch.Exchange,
		batch.Symbol,
		timestamp.UnixNano())

	key := filepath.Join(append(parts, filename)...)

	// Convert to forward slashes for S3
	return filepath.ToSlash(key)
}

func (w *S3Writer) createParquetFile(entries []models.FlattenedOrderbookEntry) ([]byte, int64, error) {
	// Filter out any entries that are missing critical fields to avoid
	// producing placeholder rows in the resulting parquet file. Some
	// upstream components may pass along entries with zero values when
	// an order book level is absent; we ignore those here.
	validEntries := make([]models.FlattenedOrderbookEntry, 0, len(entries))
	for _, e := range entries {
		if e.Timestamp.IsZero() || e.Price == 0 || e.Quantity == 0 || e.Side == "" || e.Level == 0 {
			continue
		}
		validEntries = append(validEntries, e)
	}

	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{
		"entries_count": len(validEntries),
		"operation":     "create_parquet_file",
	})
	log.Info("creating parquet file")

	// Create memory file writer
	fw := newMemoryFileWriter()

	// Create parquet writer
	pw, err := writer.NewParquetWriter(fw, new(ParquetRecord), 4)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set compression
	switch w.config.Writer.Formats.Parquet.Compression {
	case "snappy":
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	case "lzo":
		pw.CompressionType = parquet.CompressionCodec_LZO
	default:
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	}

	// Convert entries to parquet records and write
	for _, entry := range validEntries {
		record := ParquetRecord{
			Exchange:     entry.Exchange,
			Symbol:       entry.Symbol,
			Timestamp:    entry.Timestamp.UnixMilli(),
			LastUpdateID: entry.LastUpdateID,
			Side:         entry.Side,
			Price:        entry.Price,
			Quantity:     entry.Quantity,
			Level:        int32(entry.Level),
		}

		if err := pw.Write(record); err != nil {
			pw.WriteStop()
			return nil, 0, fmt.Errorf("failed to write parquet record: %w", err)
		}
	}

	// Finalize writing
	if err := pw.WriteStop(); err != nil {
		return nil, 0, fmt.Errorf("failed to finalize parquet writing: %w", err)
	}

	data := fw.Bytes()

	log.WithFields(logger.Fields{
		"file_size":     len(data),
		"entries_count": len(validEntries),
		"compression":   w.config.Writer.Formats.Parquet.Compression,
	}).Info("parquet file created successfully")

	return data, int64(len(data)), nil
}

func (w *S3Writer) uploadToS3(key string, data []byte) error {
	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{
		"operation": "upload_to_s3",
		"data_size": len(data),
	})
	log.Info("uploading to S3")

	input := &s3.PutObjectInput{
		Bucket:      aws.String(w.config.Storage.S3.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
		Metadata: map[string]string{
			"content-type":       "parquet",
			"compression":        w.config.Writer.Formats.Parquet.Compression,
			"cryptoflow-version": w.config.Cryptoflow.Version,
		},
	}

	_, err := w.s3Client.PutObject(w.ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3 bucket %s: %w", w.config.Storage.S3.Bucket, err)
	}

	log.Info("successfully uploaded to S3")
	return nil
}

func (w *S3Writer) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.reportMetrics()
		}
	}
}

func (w *S3Writer) reportMetrics() {
	batchesWritten := atomic.LoadInt64(&w.batchesWritten)
	filesWritten := atomic.LoadInt64(&w.filesWritten)
	bytesWritten := atomic.LoadInt64(&w.bytesWritten)
	errorsCount := atomic.LoadInt64(&w.errorsCount)

	errorRate := float64(0)
	if batchesWritten+errorsCount > 0 {
		errorRate = float64(errorsCount) / float64(batchesWritten+errorsCount)
	}

	avgBytesPerFile := float64(0)
	if filesWritten > 0 {
		avgBytesPerFile = float64(bytesWritten) / float64(filesWritten)
	}

	log := w.log.WithComponent("s3_writer")
	log.LogMetric("s3_writer", "batches_written", batchesWritten, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "files_written", filesWritten, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "bytes_written", bytesWritten, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "errors_count", errorsCount, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "error_rate", errorRate, "gauge", logger.Fields{})
	log.LogMetric("s3_writer", "avg_bytes_per_file", avgBytesPerFile, "gauge", logger.Fields{})

	log.WithFields(logger.Fields{
		"batches_written":    batchesWritten,
		"files_written":      filesWritten,
		"bytes_written":      bytesWritten,
		"errors_count":       errorsCount,
		"error_rate":         errorRate,
		"avg_bytes_per_file": avgBytesPerFile,
	}).Info("s3 writer metrics")
}

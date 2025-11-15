package writer

import (
	"bytes"
	"context"
	"fmt"
	"os"
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
	"cryptoflow/internal/metadata"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

type piParquetRecord struct {
	Exchange             string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Market               string  `parquet:"name=market, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol               string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventTime            int64   `parquet:"name=event_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MarkPrice            float64 `parquet:"name=mark_price, type=DOUBLE"`
	IndexPrice           float64 `parquet:"name=index_price, type=DOUBLE"`
	EstimatedSettlePrice float64 `parquet:"name=estimated_settle_price, type=DOUBLE"`
	FundingRate          float64 `parquet:"name=funding_rate, type=DOUBLE"`
	NextFundingTime      int64   `parquet:"name=next_funding_time, type=INT64"`
	PremiumIndex         float64 `parquet:"name=premium_index, type=DOUBLE"`
	ReceivedTime         int64   `parquet:"name=received_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
}

type piMemFile struct{ buffer *bytes.Buffer }

func newPiMemFile() *piMemFile                                 { return &piMemFile{buffer: &bytes.Buffer{}} }
func (m *piMemFile) Create(string) (source.ParquetFile, error)  { return m, nil }
func (m *piMemFile) Open(string) (source.ParquetFile, error)    { return m, nil }
func (m *piMemFile) Seek(int64, int) (int64, error)             { return int64(m.buffer.Len()), nil }
func (m *piMemFile) Read([]byte) (int, error)                   { return 0, fmt.Errorf("read not supported") }
func (m *piMemFile) Write(b []byte) (int, error)                { return m.buffer.Write(b) }
func (m *piMemFile) Close() error                               { return nil }
func (m *piMemFile) Bytes() []byte                              { return m.buffer.Bytes() }

// PIWriter consumes normalized premium-index batches and writes them to S3.
type PIWriter struct {
	cfg       *appconfig.Config
	normChan  <-chan models.BatchPIMessage
	s3Client  *s3.Client
	metaGen   *metadata.Generator
	log       *logger.Log
	ctx       context.Context
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	buffer    map[string][]models.NormPIMessage
	lastFlush map[string]time.Time
	flushTick *time.Ticker
	maxBuffer int
	mu        sync.Mutex
	running   bool
}

// NewPIWriter sets up the writer with credentials and metadata generator.
func NewPIWriter(cfg *appconfig.Config, norm <-chan models.BatchPIMessage) (*PIWriter, error) {
	if !cfg.Storage.S3.Enabled {
		return nil, fmt.Errorf("s3 storage disabled")
	}
	if norm == nil {
		return nil, fmt.Errorf("nil normalized channel provided")
	}

	ctx := context.Background()
	loadOpts := []func(*config.LoadOptions) error{config.WithRegion(cfg.Storage.S3.Region)}
	if cfg.Storage.S3.AccessKeyID != "" && cfg.Storage.S3.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.Storage.S3.AccessKeyID,
				cfg.Storage.S3.SecretAccessKey,
				"",
			),
		))
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

	metaDir, err := os.MkdirTemp("", "pi-metadata")
	if err != nil {
		return nil, fmt.Errorf("create metadata dir: %w", err)
	}
	tablePrefix := filepath.ToSlash(filepath.Join("premium_index"))
	location := fmt.Sprintf("s3://%s/%s", cfg.Storage.S3.Bucket, tablePrefix)
	meta := metadata.NewGenerator(metaDir, location, cfg.Storage.S3.Bucket, tablePrefix, cfg.Cryptoflow.Name+"_pi", s3Client)

	maxBuffer := cfg.Writer.Buffer.MaxSize
	if maxBuffer <= 0 {
		maxBuffer = 512
	}

	return &PIWriter{
		cfg:       cfg,
		normChan:  norm,
		s3Client:  s3Client,
		metaGen:   meta,
		log:       logger.GetLogger(),
		wg:        &sync.WaitGroup{},
		buffer:    make(map[string][]models.NormPIMessage),
		lastFlush: make(map[string]time.Time),
		maxBuffer: maxBuffer,
	}, nil
}

// Start launches the ingest and flush goroutines.
func (w *PIWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("pi writer already running")
	}
	w.running = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	interval := w.cfg.Writer.Buffer.SnapshotFlushInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	w.flushTick = time.NewTicker(interval)
	w.buffer = make(map[string][]models.NormPIMessage)
	w.lastFlush = make(map[string]time.Time)
	w.mu.Unlock()

	w.log.WithComponent("pi_writer").WithFields(logger.Fields{
		"flush_interval": interval,
		"max_buffer":     w.maxBuffer,
	}).Info("starting premium-index writer")

	w.wg.Add(1)
	go w.ingest()

	w.wg.Add(1)
	go w.flushLoop()
	return nil
}

// Stop terminates workers and flushes pending entries.
func (w *PIWriter) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	cancel := w.cancel
	ticker := w.flushTick
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ticker != nil {
		ticker.Stop()
	}

	w.flushAll("shutdown")
	w.wg.Wait()
	w.log.WithComponent("pi_writer").Info("premium-index writer stopped")
}

func (w *PIWriter) ingest() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case batch, ok := <-w.normChan:
			if !ok {
				w.flushAll("norm_channel_closed")
				return
			}
			w.addBatch(batch)
		}
	}
}

func (w *PIWriter) addBatch(batch models.BatchPIMessage) {
	key := bufferKey(batch.Exchange, batch.Market, batch.Symbol)
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], batch.Entries...)
	if _, ok := w.lastFlush[key]; !ok {
		w.lastFlush[key] = time.Now()
	}
	shouldFlush := len(w.buffer[key]) >= w.maxBuffer
	w.mu.Unlock()

	if shouldFlush {
		w.flushKey(key)
	}
}

func (w *PIWriter) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.flushTick.C:
			w.flushTimedOut()
		}
	}
}

func (w *PIWriter) flushTimedOut() {
	w.mu.Lock()
	now := time.Now()
	keys := make([]string, 0, len(w.lastFlush))
	for key, ts := range w.lastFlush {
		if now.Sub(ts) >= w.cfg.Writer.Buffer.SnapshotFlushInterval && len(w.buffer[key]) > 0 {
			keys = append(keys, key)
		}
	}
	w.mu.Unlock()
	for _, key := range keys {
		w.flushKey(key)
	}
}

func (w *PIWriter) flushAll(reason string) {
	w.mu.Lock()
	keys := make([]string, 0, len(w.buffer))
	for key := range w.buffer {
		if len(w.buffer[key]) > 0 {
			keys = append(keys, key)
		}
	}
	w.mu.Unlock()

	if len(keys) == 0 {
		return
	}

	w.log.WithComponent("pi_writer").WithFields(logger.Fields{
		"flushed_buffers": len(keys),
		"reason":          reason,
	}).Info("flushing premium-index buffers")

	for _, key := range keys {
		w.flushKey(key)
	}
}

func (w *PIWriter) flushKey(key string) {
	w.mu.Lock()
	entries := w.buffer[key]
	if len(entries) == 0 {
		w.mu.Unlock()
		return
	}
	delete(w.buffer, key)
	delete(w.lastFlush, key)
	w.mu.Unlock()

	parts := strings.SplitN(key, "|", 3)
	exchange := parts[0]
	market := ""
	symbol := ""
	if len(parts) > 1 {
		market = parts[1]
	}
	if len(parts) > 2 {
		symbol = parts[2]
	}

	batch := models.BatchPIMessage{
		BatchID:     uuid.New().String(),
		Exchange:    exchange,
		Market:      market,
		Symbol:      symbol,
		Entries:     entries,
		RecordCount: len(entries),
		Timestamp:   time.Now().UTC(),
		ProcessedAt: time.Now(),
	}
	w.writeBatch(batch)
}

func (w *PIWriter) writeBatch(batch models.BatchPIMessage) {
	data, size, err := w.createParquet(batch)
	if err != nil {
		w.log.WithComponent("pi_writer").WithError(err).Error("failed to create PI parquet")
		return
	}
	key := w.s3Key(batch)
	if err := w.upload(key, data); err != nil {
		w.log.WithComponent("pi_writer").WithError(err).WithFields(logger.Fields{
			"s3_key": key,
		}).Error("failed to upload PI batch")
		return
	}

	if w.metaGen != nil {
		df := metadata.DataFile{
			Path:        fmt.Sprintf("s3://%s/%s", w.cfg.Storage.S3.Bucket, key),
			FileSize:    size,
			RecordCount: int64(len(batch.Entries)),
			Partition: map[string]any{
				"exchange": batch.Exchange,
				"market":   batch.Market,
				"symbol":   batch.Symbol,
				"date":     batch.Timestamp.UTC().Format("2006-01-02"),
			},
			Timestamp: batch.Timestamp,
		}
		if err := w.metaGen.AddFile(df); err != nil {
			w.log.WithComponent("pi_writer").WithError(err).Warn("failed to update PI metadata")
		}
	}

	w.log.WithComponent("pi_writer").WithFields(logger.Fields{
		"s3_key":  key,
		"records": batch.RecordCount,
		"bytes":   size,
	}).Info("premium-index batch uploaded")
}

func (w *PIWriter) createParquet(batch models.BatchPIMessage) ([]byte, int64, error) {
	mem := newPiMemFile()
	pw, err := writer.NewParquetWriter(mem, new(piParquetRecord), 1)
	if err != nil {
		return nil, 0, fmt.Errorf("new parquet writer: %w", err)
	}

	switch strings.ToLower(w.cfg.Writer.Formats.Parquet.Compression) {
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	case "snappy", "":
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	default:
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	}

	for _, entry := range batch.Entries {
		rec := piParquetRecord{
			Exchange:             strings.ToLower(batch.Exchange),
			Market:               batch.Market,
			Symbol:               batch.Symbol,
			EventTime:            entry.EventTime,
			MarkPrice:            entry.MarkPrice,
			IndexPrice:           entry.IndexPrice,
			EstimatedSettlePrice: entry.EstimatedSettlePrice,
			FundingRate:          entry.FundingRate,
			NextFundingTime:      entry.NextFundingTime,
			PremiumIndex:         entry.PremiumIndex,
			ReceivedTime:         entry.ReceivedTime,
		}
		if err := pw.Write(rec); err != nil {
			pw.WriteStop()
			return nil, 0, fmt.Errorf("write parquet record: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, 0, fmt.Errorf("finalize parquet: %w", err)
	}
	data := mem.Bytes()
	return data, int64(len(data)), nil
}

func (w *PIWriter) upload(key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Storage.S3.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
		Metadata: map[string]string{
			"content-type":       "parquet",
			"compression":        w.cfg.Writer.Formats.Parquet.Compression,
			"cryptoflow-version": w.cfg.Cryptoflow.Version,
		},
	}
	ctx, cancel := context.WithTimeout(w.ctx, 2*time.Minute)
	defer cancel()
	_, err := w.s3Client.PutObject(ctx, input)
	return err
}

func (w *PIWriter) s3Key(batch models.BatchPIMessage) string {
	timestamp := batch.Timestamp.UTC()
	var parts []string
	for _, key := range w.cfg.Writer.Partitioning.AdditionalKeys {
		switch key {
		case "exchange":
			if batch.Exchange != "" {
				parts = append(parts, fmt.Sprintf("exchange=%s", strings.ToLower(batch.Exchange)))
			}
		case "market":
			if batch.Market != "" {
				parts = append(parts, fmt.Sprintf("market=%s", batch.Market))
			}
		case "symbol":
			if batch.Symbol != "" {
				parts = append(parts, fmt.Sprintf("symbol=%s", strings.ToUpper(batch.Symbol)))
			}
		}
	}

	timeFormat := w.cfg.Writer.Partitioning.TimeFormat
	if timeFormat == "" {
		timeFormat = "date={year}-{month}-{day}"
	}
	timePath := strings.ReplaceAll(timeFormat, "{year}", fmt.Sprintf("%04d", timestamp.Year()))
	timePath = strings.ReplaceAll(timePath, "{month}", fmt.Sprintf("%02d", timestamp.Month()))
	timePath = strings.ReplaceAll(timePath, "{day}", fmt.Sprintf("%02d", timestamp.Day()))
	timePath = strings.ReplaceAll(timePath, "{hour}", fmt.Sprintf("%02d", timestamp.Hour()))

	parts = append(parts, timePath)
	filename := fmt.Sprintf("%s_pi_%s_%s.parquet",
		strings.ToLower(batch.Exchange),
		strings.ToUpper(batch.Symbol),
		timestamp.Format("20060102150405")+uuid.NewString(),
	)
	return filepath.ToSlash(filepath.Join(append(parts, filename)...))
}

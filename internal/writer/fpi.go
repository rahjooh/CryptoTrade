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
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	pq "github.com/xitongsys/parquet-go/writer"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

type fpiParquetRecord struct {
	Exchange             string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Market               string  `parquet:"name=market, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol               string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventTimeMs          int64   `parquet:"name=event_time_ms, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MarkPrice            float64 `parquet:"name=mark_price, type=DOUBLE"`
	IndexPrice           float64 `parquet:"name=index_price, type=DOUBLE"`
	EstimatedSettlePrice float64 `parquet:"name=estimated_settle_price, type=DOUBLE"`
	FundingRate          float64 `parquet:"name=funding_rate, type=DOUBLE"`
	NextFundingTimeMs    int64   `parquet:"name=next_funding_time_ms, type=INT64"`
	PremiumIndex         float64 `parquet:"name=premium_index, type=DOUBLE"`
	ReceivedTimeMs       int64   `parquet:"name=received_time_ms, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Source               string  `parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type fpiMemoryFile struct {
	buffer *bytes.Buffer
}

func newFPIMemoryFile() *fpiMemoryFile {
	return &fpiMemoryFile{buffer: &bytes.Buffer{}}
}

func (m *fpiMemoryFile) Create(string) (source.ParquetFile, error) { return m, nil }
func (m *fpiMemoryFile) Open(string) (source.ParquetFile, error)   { return m, nil }
func (m *fpiMemoryFile) Seek(int64, int) (int64, error)            { return int64(m.buffer.Len()), nil }
func (m *fpiMemoryFile) Read(b []byte) (int, error)                { return m.buffer.Read(b) }
func (m *fpiMemoryFile) Write(b []byte) (int, error)               { return m.buffer.Write(b) }
func (m *fpiMemoryFile) Close() error                              { return nil }
func (m *fpiMemoryFile) Bytes() []byte                             { return m.buffer.Bytes() }

type fpiBatch struct {
	Exchange string
	Market   string
	Symbol   string
	Records  []models.NormFPI
}

// FPIWriter consumes normalized premium-index messages and writes Parquet files to S3.
type FPIWriter struct {
	cfg      *appconfig.Config
	normChan <-chan models.NormFPI
	s3Client *s3.Client
	log      *logger.Log

	ctx     context.Context
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
	mu      sync.Mutex
	running bool

	buffer      map[string][]models.NormFPI
	flushTicker *time.Ticker
	maxBuffer   int
}

// NewFPIWriter sets up the writer with AWS credentials.
func NewFPIWriter(cfg *appconfig.Config, norm <-chan models.NormFPI) (*FPIWriter, error) {
	if !cfg.Storage.S3.Enabled {
		return nil, fmt.Errorf("s3 storage disabled")
	}
	if norm == nil {
		return nil, fmt.Errorf("nil normalized channel provided")
	}

	ctx := context.Background()
	opts := []func(*config.LoadOptions) error{config.WithRegion(cfg.Storage.S3.Region)}
	if cfg.Storage.S3.AccessKeyID != "" && cfg.Storage.S3.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.Storage.S3.AccessKeyID,
				cfg.Storage.S3.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Storage.S3.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Storage.S3.Endpoint)
		}
		o.UsePathStyle = cfg.Storage.S3.PathStyle
	})

	maxBuf := cfg.Writer.Buffer.MaxSize
	if maxBuf <= 0 {
		maxBuf = 1024
	}

	return &FPIWriter{
		cfg:       cfg,
		normChan:  norm,
		s3Client:  client,
		log:       logger.GetLogger(),
		wg:        &sync.WaitGroup{},
		buffer:    make(map[string][]models.NormFPI),
		maxBuffer: maxBuf,
	}, nil
}

// Start launches ingestion and flush loops.
func (w *FPIWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("fpi writer already running")
	}
	w.running = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	interval := w.cfg.Writer.Buffer.FPIFlushInterval
	if interval <= 0 {
		interval = time.Minute
	}
	w.flushTicker = time.NewTicker(interval)
	w.buffer = make(map[string][]models.NormFPI)
	w.mu.Unlock()

	w.log.WithComponent("fpi_writer").WithFields(logger.Fields{
		"flush_interval": interval,
		"max_buffer":     w.maxBuffer,
	}).Info("starting FPI writer")

	w.wg.Add(1)
	go w.ingest()

	w.wg.Add(1)
	go w.flushLoop()

	return nil
}

// Stop cancels workers and flushes pending entries.
func (w *FPIWriter) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	cancel := w.cancel
	ticker := w.flushTicker
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ticker != nil {
		ticker.Stop()
	}

	w.flushAll("shutdown")
	w.wg.Wait()
	w.log.WithComponent("fpi_writer").Info("FPI writer stopped")
}

func (w *FPIWriter) ingest() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case msg, ok := <-w.normChan:
			if !ok {
				w.flushAll("norm_channel_closed")
				return
			}
			w.add(msg)
		}
	}
}

func (w *FPIWriter) add(msg models.NormFPI) {
	key := fmt.Sprintf("%s|%s|%s", msg.Exchange, msg.Market, msg.Symbol)
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], msg)
	count := len(w.buffer[key])
	w.mu.Unlock()

	if count >= w.maxBuffer {
		w.flushKey(key)
	}
}

func (w *FPIWriter) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.flushTicker.C:
			w.flushTimedOut()
		}
	}
}

func (w *FPIWriter) flushTimedOut() {
	w.mu.Lock()
	buffers := w.buffer
	w.buffer = make(map[string][]models.NormFPI)
	w.mu.Unlock()

	for key, entries := range buffers {
		if len(entries) == 0 {
			continue
		}
		w.processBatch(key, entries)
	}
}

func (w *FPIWriter) flushAll(reason string) {
	w.mu.Lock()
	buffers := w.buffer
	w.buffer = make(map[string][]models.NormFPI)
	w.mu.Unlock()

	if len(buffers) == 0 {
		return
	}

	w.log.WithComponent("fpi_writer").WithFields(logger.Fields{
		"buffers": len(buffers),
		"reason":  reason,
	}).Info("flushing FPI buffers")

	for key, entries := range buffers {
		if len(entries) == 0 {
			continue
		}
		w.processBatch(key, entries)
	}
}

func (w *FPIWriter) flushKey(key string) {
	w.mu.Lock()
	entries := w.buffer[key]
	w.buffer[key] = nil
	w.mu.Unlock()

	if len(entries) == 0 {
		return
	}
	w.processBatch(key, entries)
}

func (w *FPIWriter) processBatch(key string, entries []models.NormFPI) {
	parts := strings.SplitN(key, "|", 3)
	exchange, market, symbol := parts[0], parts[1], parts[2]

	batch := fpiBatch{
		Exchange: exchange,
		Market:   market,
		Symbol:   symbol,
		Records:  entries,
	}

	data, err := w.createParquet(batch)
	if err != nil {
		w.log.WithComponent("fpi_writer").WithError(err).Error("failed to create FPI parquet")
		return
	}

	keyPath := w.generateS3Key(batch, entries[len(entries)-1].EventTimeMs)
	if err := w.upload(keyPath, data); err != nil {
		w.log.WithComponent("fpi_writer").WithError(err).WithFields(logger.Fields{
			"s3_key": keyPath,
		}).Error("failed to upload FPI parquet")
		return
	}

	w.log.WithComponent("fpi_writer").WithFields(logger.Fields{
		"s3_key":   keyPath,
		"records":  len(entries),
		"exchange": exchange,
		"symbol":   symbol,
	}).Info("uploaded FPI parquet batch")
}

func (w *FPIWriter) createParquet(batch fpiBatch) ([]byte, error) {
	mem := newFPIMemoryFile()
	pw, err := pq.NewParquetWriter(mem, new(fpiParquetRecord), 4)
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	switch strings.ToLower(w.cfg.Writer.Formats.Parquet.Compression) {
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	case "snappy", "":
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	default:
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	}

	for _, entry := range batch.Records {
		rec := fpiParquetRecord{
			Exchange:             batch.Exchange,
			Market:               batch.Market,
			Symbol:               batch.Symbol,
			EventTimeMs:          entry.EventTimeMs,
			MarkPrice:            entry.MarkPrice,
			IndexPrice:           entry.IndexPrice,
			EstimatedSettlePrice: entry.EstimatedSettlePrice,
			FundingRate:          entry.FundingRate,
			NextFundingTimeMs:    entry.NextFundingTimeMs,
			PremiumIndex:         entry.PremiumIndex,
			ReceivedTimeMs:       entry.ReceivedTimeMs,
			Source:               entry.Source,
		}
		if err := pw.Write(rec); err != nil {
			return nil, fmt.Errorf("write parquet record: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, fmt.Errorf("finalise parquet: %w", err)
	}

	return mem.Bytes(), nil
}

func (w *FPIWriter) upload(key string, data []byte) error {
	_, err := w.s3Client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Storage.S3.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (w *FPIWriter) generateS3Key(batch fpiBatch, eventTime int64) string {
	timestamp := time.UnixMilli(eventTime).UTC()

	var parts []string
	for _, k := range w.cfg.Writer.Partitioning.AdditionalKeys {
		switch k {
		case "exchange":
			parts = append(parts, fmt.Sprintf("exchange=%s", batch.Exchange))
		case "market":
			if batch.Market != "" {
				parts = append(parts, fmt.Sprintf("market=%s", batch.Market))
			}
		case "symbol":
			parts = append(parts, fmt.Sprintf("symbol=%s", batch.Symbol))
		}
	}

	timeFormat := w.cfg.Writer.Partitioning.TimeFormat
	if timeFormat == "" {
		timeFormat = "date={year}-{month}-{day}"
	}
	timePath := strings.ReplaceAll(timeFormat, "{year}", fmt.Sprintf("%04d", timestamp.Year()))
	timePath = strings.ReplaceAll(timePath, "{month}", fmt.Sprintf("%02d", int(timestamp.Month())))
	timePath = strings.ReplaceAll(timePath, "{day}", fmt.Sprintf("%02d", timestamp.Day()))
	timePath = strings.ReplaceAll(timePath, "{hour}", fmt.Sprintf("%02d", timestamp.Hour()))
	parts = append(parts, timePath)

	filename := fmt.Sprintf("%s_fpi_%s_%s.parquet",
		batch.Exchange,
		batch.Symbol,
		timestamp.Format("20060102T150405"),
	)

	return filepath.ToSlash(filepath.Join(append(parts, filename)...))
}

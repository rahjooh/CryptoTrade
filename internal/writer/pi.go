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
	Timestamp            int64   `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MarkPrice            float64 `parquet:"name=mark_price, type=DOUBLE"`
	IndexPrice           float64 `parquet:"name=index_price, type=DOUBLE"`
	EstimatedSettlePrice float64 `parquet:"name=estimated_settle_price, type=DOUBLE"`
	FundingRate          float64 `parquet:"name=funding_rate, type=DOUBLE"`
	NextFundingTime      int64   `parquet:"name=next_funding_time, type=INT64"`
	PremiumIndex         float64 `parquet:"name=premium_index, type=DOUBLE"`
	Source               string  `parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type piBatch struct {
	Exchange    string
	Market      string
	Symbol      string
	Entries     []models.RawPIMessage
	Timestamp   time.Time
	Reason      string
	RecordCount int
}

type piMemFile struct {
	buffer *bytes.Buffer
}

func newPiMemFile() *piMemFile {
	return &piMemFile{buffer: &bytes.Buffer{}}
}

func (m *piMemFile) Create(string) (source.ParquetFile, error) { return m, nil }
func (m *piMemFile) Open(string) (source.ParquetFile, error)   { return m, nil }
func (m *piMemFile) Seek(int64, int) (int64, error)            { return int64(m.buffer.Len()), nil }
func (m *piMemFile) Read([]byte) (int, error)                  { return 0, fmt.Errorf("read not supported") }
func (m *piMemFile) Write(b []byte) (int, error)               { return m.buffer.Write(b) }
func (m *piMemFile) Close() error                              { return nil }
func (m *piMemFile) Bytes() []byte                             { return m.buffer.Bytes() }

// PIWriter uploads premium-index snapshots to S3 as parquet files.
type PIWriter struct {
	cfg      *appconfig.Config
	rawChan  <-chan models.RawPIMessage
	s3Client *s3.Client
	metaGen  *metadata.Generator

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	log *logger.Log

	mu          sync.Mutex
	buffer      map[string][]models.RawPIMessage
	flushTicker *time.Ticker
	maxBuffer   int
	jobCh       chan piBatch
	running     bool
}

// NewPIWriter creates a new premium-index writer.
func NewPIWriter(cfg *appconfig.Config, raw <-chan models.RawPIMessage) (*PIWriter, error) {
	if !cfg.Storage.S3.Enabled {
		return nil, fmt.Errorf("s3 storage disabled")
	}
	if raw == nil {
		return nil, fmt.Errorf("nil raw channel provided")
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

	jobCapacity := maxBuffer * 2
	if jobCapacity < 128 {
		jobCapacity = 128
	}

	return &PIWriter{
		cfg:       cfg,
		rawChan:   raw,
		s3Client:  s3Client,
		metaGen:   meta,
		wg:        &sync.WaitGroup{},
		log:       logger.GetLogger(),
		buffer:    make(map[string][]models.RawPIMessage),
		maxBuffer: maxBuffer,
		jobCh:     make(chan piBatch, jobCapacity),
	}, nil
}

// Start begins processing raw messages.
func (w *PIWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("pi writer already running")
	}
	w.running = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.buffer = make(map[string][]models.RawPIMessage)
	w.flushTicker = time.NewTicker(w.cfg.Writer.Buffer.SnapshotFlushInterval)
	w.mu.Unlock()

	w.log.WithComponent("pi_writer").WithFields(logger.Fields{
		"flush_interval": w.cfg.Writer.Buffer.SnapshotFlushInterval,
		"max_buffer":     w.maxBuffer,
	}).Info("starting premium-index writer")

	w.wg.Add(1)
	go w.ingest()

	w.wg.Add(1)
	go w.flushLoop()

	workers := w.cfg.Writer.MaxWorkers
	if workers <= 0 {
		workers = 2
	}
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go w.uploadWorker(i)
	}
	return nil
}

// Stop terminates goroutines and flushes buffers.
func (w *PIWriter) Stop() {
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

	w.flushBuffers("shutdown")
	close(w.jobCh)
	w.wg.Wait()
	w.log.WithComponent("pi_writer").Info("premium-index writer stopped")
}

func (w *PIWriter) ingest() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case msg, ok := <-w.rawChan:
			if !ok {
				w.flushBuffers("channel_closed")
				return
			}
			if msg.Market == "" {
				msg.Market = "pi"
			}
			if msg.Timestamp.IsZero() {
				msg.Timestamp = time.Now().UTC()
			}
			w.addMessage(msg)
		}
	}
}

func (w *PIWriter) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.flushTicker.C:
			w.flushBuffers("interval")
		}
	}
}

func (w *PIWriter) uploadWorker(id int) {
	defer w.wg.Done()
	for batch := range w.jobCh {
		w.processBatch(batch)
	}
}

func (w *PIWriter) addMessage(msg models.RawPIMessage) {
	key := w.bufferKey(msg.Exchange, msg.Market, msg.Symbol)

	var flushEntries []models.RawPIMessage
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], msg)
	if len(w.buffer[key]) >= w.maxBuffer {
		flushEntries = w.buffer[key]
		delete(w.buffer, key)
	}
	w.mu.Unlock()

	if len(flushEntries) > 0 {
		w.enqueueBatch(key, flushEntries, "max_buffer")
	}
}

func (w *PIWriter) flushBuffers(reason string) {
	w.mu.Lock()
	buffers := w.buffer
	w.buffer = make(map[string][]models.RawPIMessage)
	w.mu.Unlock()

	for key, entries := range buffers {
		if len(entries) == 0 {
			continue
		}
		w.enqueueBatch(key, entries, reason)
	}
}

func (w *PIWriter) enqueueBatch(key string, entries []models.RawPIMessage, reason string) {
	batch := w.makeBatch(key, entries, reason)
	select {
	case w.jobCh <- batch:
	case <-w.ctx.Done():
	}
}

func (w *PIWriter) makeBatch(key string, entries []models.RawPIMessage, reason string) piBatch {
	parts := strings.SplitN(key, "|", 3)
	exchange, market, symbol := parts[0], "", ""
	if len(parts) > 1 {
		market = parts[1]
	}
	if len(parts) > 2 {
		symbol = parts[2]
	}
	if market == "" {
		market = "pi"
	}
	ts := time.Now().UTC()
	if len(entries) > 0 && !entries[len(entries)-1].Timestamp.IsZero() {
		ts = entries[len(entries)-1].Timestamp
	}
	return piBatch{
		Exchange:    exchange,
		Market:      market,
		Symbol:      symbol,
		Entries:     entries,
		Timestamp:   ts,
		Reason:      reason,
		RecordCount: len(entries),
	}
}

func (w *PIWriter) bufferKey(exchange, market, symbol string) string {
	return strings.Join([]string{
		strings.ToLower(exchange),
		strings.ToLower(market),
		strings.ToUpper(symbol),
	}, "|")
}

func (w *PIWriter) processBatch(batch piBatch) {
	entryLog := w.log.WithComponent("pi_writer").WithFields(logger.Fields{
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"record_count": batch.RecordCount,
		"reason":       batch.Reason,
	})

	if batch.RecordCount == 0 {
		entryLog.Debug("premium-index batch empty, skipping")
		return
	}

	key := w.generateS3Key(batch)
	data, size, err := w.createParquet(batch)
	if err != nil {
		entryLog.WithError(err).Error("failed to create premium-index parquet")
		return
	}

	if err := w.uploadToS3(key, data); err != nil {
		entryLog.WithError(err).WithFields(logger.Fields{"key": key}).Error("failed to upload premium-index parquet")
		return
	}

	df := metadata.DataFile{
		Path:        fmt.Sprintf("s3://%s/%s", w.cfg.Storage.S3.Bucket, key),
		FileSize:    size,
		RecordCount: int64(batch.RecordCount),
		Partition: map[string]any{
			"exchange": batch.Exchange,
			"market":   batch.Market,
			"symbol":   batch.Symbol,
			"date":     batch.Timestamp.UTC().Format("2006-01-02"),
		},
		Timestamp: batch.Timestamp,
	}
	if w.metaGen != nil {
		if err := w.metaGen.AddFile(df); err != nil {
			entryLog.WithError(err).Warn("failed to update premium-index metadata")
		}
	}

	entryLog.WithFields(logger.Fields{
		"s3_key":    key,
		"file_size": size,
	}).Info("premium-index batch uploaded")
}

func (w *PIWriter) createParquet(batch piBatch) ([]byte, int64, error) {
	records := make([]piParquetRecord, 0, len(batch.Entries))
	for _, entry := range batch.Entries {
		ts := entry.Timestamp
		if ts.IsZero() {
			ts = batch.Timestamp
		}
		nextFunding := int64(0)
		if !entry.NextFundingTime.IsZero() {
			nextFunding = entry.NextFundingTime.UnixMilli()
		}
		record := piParquetRecord{
			Exchange:             entry.Exchange,
			Market:               entry.Market,
			Symbol:               entry.Symbol,
			Timestamp:            ts.UnixMilli(),
			MarkPrice:            entry.MarkPrice,
			IndexPrice:           entry.IndexPrice,
			EstimatedSettlePrice: entry.EstimatedSettlePrice,
			FundingRate:          entry.FundingRate,
			NextFundingTime:      nextFunding,
			PremiumIndex:         entry.PremiumIndex,
			Source:               entry.Source,
		}
		if record.Market == "" {
			record.Market = "pi"
		}
		records = append(records, record)
	}

	mem := newPiMemFile()
	pw, err := writer.NewParquetWriter(mem, new(piParquetRecord), 1)
	if err != nil {
		return nil, 0, fmt.Errorf("new parquet writer: %w", err)
	}

	switch strings.ToLower(w.cfg.Writer.Formats.Parquet.Compression) {
	case "snappy":
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	default:
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	}

	for _, rec := range records {
		if err := pw.Write(rec); err != nil {
			pw.WriteStop()
			return nil, 0, fmt.Errorf("write premium-index record: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, 0, fmt.Errorf("finalize premium-index parquet: %w", err)
	}

	data := mem.Bytes()
	return data, int64(len(data)), nil
}

func (w *PIWriter) generateS3Key(batch piBatch) string {
	datePart := batch.Timestamp.UTC().Format("2006-01-02")
	filename := fmt.Sprintf("%s_%s_%s_pi.parquet",
		strings.ToLower(batch.Exchange),
		strings.ToUpper(batch.Symbol),
		time.Now().UTC().Format("20060102150405")+uuid.NewString(),
	)
	key := filepath.Join(
		fmt.Sprintf("exchange=%s", strings.ToLower(batch.Exchange)),
		fmt.Sprintf("market=%s", batch.Market),
		fmt.Sprintf("symbol=%s", strings.ToUpper(batch.Symbol)),
		fmt.Sprintf("date=%s", datePart),
		filename,
	)
	return filepath.ToSlash(key)
}

func (w *PIWriter) uploadToS3(key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(w.cfg.Storage.S3.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
		Metadata: map[string]string{
			"content-type":       "parquet",
			"compression":        w.cfg.Writer.Formats.Parquet.Compression,
			"cryptoflow-version": w.cfg.Cryptoflow.Version,
		},
	}

	ctx, cancel := context.WithTimeout(w.ctx, 2*time.Minute)
	defer cancel()
	if _, err := w.s3Client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("upload premium-index parquet: %w", err)
	}
	return nil
}

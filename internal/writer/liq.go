package writer

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/xitongsys/parquet-go/writer"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

const (
	liquidationKeySeparator = "|"
	defaultLiquidationFlush = time.Minute
)

type liquidationMemFile struct {
	buffer *bytes.Buffer
}

func newLiquidationMemFile() *liquidationMemFile {
	return &liquidationMemFile{buffer: &bytes.Buffer{}}
}

func (m *liquidationMemFile) Create(string) (source.ParquetFile, error) { return m, nil }
func (m *liquidationMemFile) Open(string) (source.ParquetFile, error)   { return m, nil }
func (m *liquidationMemFile) Seek(int64, int) (int64, error)            { return int64(m.buffer.Len()), nil }
func (m *liquidationMemFile) Read([]byte) (int, error)                  { return 0, io.EOF }
func (m *liquidationMemFile) Write(b []byte) (int, error)               { return m.buffer.Write(b) }
func (m *liquidationMemFile) Close() error                              { return nil }
func (m *liquidationMemFile) Bytes() []byte                             { return m.buffer.Bytes() }

// liquidationRecord defines the schema for liquidation payloads stored in parquet.
type liquidationRecord struct {
	Exchange    string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Market      string  `parquet:"name=market, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol      string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventTime   int64   `parquet:"name=event_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Side        string  `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8"`
	OrderType   string  `parquet:"name=order_type, type=BYTE_ARRAY, convertedtype=UTF8"`
	Price       float64 `parquet:"name=price, type=DOUBLE"`
	Quantity    float64 `parquet:"name=quantity, type=DOUBLE"`
	ReceivedTime int64  `parquet:"name=received_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Payload     string  `parquet:"name=payload, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type liquidationBatch struct {
	Exchange    string
	Market      string
	Symbol      string
	Entries     []models.NormLiquidationMessage
	Timestamp   time.Time
	RecordCount int
}

// LiquidationWriter buffers liquidation payloads and periodically writes them to S3
// using snappy-compressed parquet files.
type LiquidationWriter struct {
	cfg            *appconfig.Config
	normChan       <-chan models.BatchLiquidationMessage
	s3Client       *s3.Client
	log            *logger.Log
	bucket         string
	ctx            context.Context
	cancel         context.CancelFunc
	wg             *sync.WaitGroup
	running        bool
	mu             sync.Mutex
	buffer         map[string][]models.NormLiquidationMessage
	lastFlush      map[string]time.Time
	flushIntervals map[string]time.Duration
	flushTicker    *time.Ticker
	maxBufferSize  int
}

// NewLiquidationWriter initializes the writer using S3 credentials from config and
// prepares buffering structures.
func NewLiquidationWriter(cfg *appconfig.Config, normChan <-chan models.BatchLiquidationMessage) (*LiquidationWriter, error) {
	log := logger.GetLogger()
	if !cfg.Storage.S3.Enabled {
		return nil, fmt.Errorf("s3 storage is disabled")
	}

	bucket, err := normalizeBucketName(cfg.Storage.S3.Bucket)
	if err != nil {
		return nil, err
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

	writer := &LiquidationWriter{
		cfg:            cfg,
		normChan:       normChan,
		s3Client:       s3Client,
		log:            log,
		bucket:         bucket,
		wg:             &sync.WaitGroup{},
		buffer:         make(map[string][]models.NormLiquidationMessage),
		lastFlush:      make(map[string]time.Time),
		flushIntervals: buildLiquidationIntervals(cfg),
	}

	if cfg.Writer.Buffer.MaxSize > 0 {
		writer.maxBufferSize = cfg.Writer.Buffer.MaxSize
	} else {
		writer.maxBufferSize = 100
	}

	log.WithComponent("liq_writer").WithFields(logger.Fields{
		"bucket":     bucket,
		"region":     cfg.Storage.S3.Region,
		"endpoint":   cfg.Storage.S3.Endpoint,
		"path_style": cfg.Storage.S3.PathStyle,
	}).Info("liquidation writer initialized")

	return writer, nil
}

func normalizeBucketName(raw string) (string, error) {
	bucket := strings.TrimSpace(raw)
	if bucket == "" {
		return "", fmt.Errorf("s3 bucket not configured")
	}
	return bucket, nil
}

// Start launches the ingestion and flush workers.
func (w *LiquidationWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("liquidation writer already running")
	}
	w.running = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.buffer = make(map[string][]models.NormLiquidationMessage)
	w.lastFlush = make(map[string]time.Time)
	tickerInterval := w.determineTickerInterval()
	w.flushTicker = time.NewTicker(tickerInterval)
	w.mu.Unlock()

	w.log.WithComponent("liq_writer").WithFields(logger.Fields{
		"ticker_interval": tickerInterval.String(),
		"max_buffer":      w.maxBufferSize,
	}).Info("starting liquidation writer")

	w.wg.Add(1)
	go w.worker()

	w.wg.Add(1)
	go w.flushWorker()

	return nil
}

// Stop signals the workers to terminate and flushes remaining data.
func (w *LiquidationWriter) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	cancel := w.cancel
	ticker := w.flushTicker
	w.cancel = nil
	w.flushTicker = nil
	w.mu.Unlock()

	if ticker != nil {
		ticker.Stop()
	}
	if cancel != nil {
		cancel()
	}

	w.wg.Wait()
	w.flushAll("stop")
	w.log.WithComponent("liq_writer").Info("liquidation writer stopped")
}

func (w *LiquidationWriter) worker() {
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

func (w *LiquidationWriter) flushWorker() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			w.flushAll("context_cancelled")
			return
		case <-w.flushTicker.C:
			w.flushTimedOut()
		}
	}
}

func (w *LiquidationWriter) addBatch(batch models.BatchLiquidationMessage) {
	if batch.Exchange == "" || batch.Symbol == "" {
		return
	}
	key := w.bufferKey(batch.Exchange, batch.Market, batch.Symbol)
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], batch.Entries...)
	if _, ok := w.lastFlush[key]; !ok {
		w.lastFlush[key] = time.Now()
	}
	shouldFlush := w.maxBufferSize > 0 && len(w.buffer[key]) >= w.maxBufferSize
	w.mu.Unlock()

	if shouldFlush {
		w.flushKey(key)
	}
}

func (w *LiquidationWriter) flushTimedOut() {
	now := time.Now()
	w.mu.Lock()
	keys := make([]string, 0, len(w.buffer))
	for key := range w.buffer {
		last := w.lastFlush[key]
		interval := w.intervalForKey(key)
		if len(w.buffer[key]) == 0 {
			continue
		}
		if now.Sub(last) >= interval {
			keys = append(keys, key)
		}
	}
	w.mu.Unlock()

	for _, key := range keys {
		w.flushKey(key)
	}
}

func (w *LiquidationWriter) flushAll(reason string) {
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

	w.log.WithComponent("liq_writer").WithFields(logger.Fields{
		"flushed_buffers": len(keys),
		"reason":          reason,
	}).Info("flushing liquidation buffers")

	for _, key := range keys {
		w.flushKey(key)
	}
}

func (w *LiquidationWriter) flushKey(key string) {
	w.mu.Lock()
	entries := w.buffer[key]
	if len(entries) == 0 {
		w.mu.Unlock()
		return
	}
	delete(w.buffer, key)
	delete(w.lastFlush, key)
	w.mu.Unlock()

	parts := strings.SplitN(key, liquidationKeySeparator, 3)
	exchange := parts[0]
	market := "liquidation"
	symbol := ""
	if len(parts) > 1 && parts[1] != "" {
		market = parts[1]
	}
	if len(parts) > 2 {
		symbol = parts[2]
	}

	var batchTimestamp time.Time
	for _, entry := range entries {
		if entry.EventTime > 0 {
			ts := time.UnixMilli(entry.EventTime)
			if ts.After(batchTimestamp) {
				batchTimestamp = ts
			}
		}
	}
	if batchTimestamp.IsZero() {
		batchTimestamp = time.Now().UTC()
	}

	batch := liquidationBatch{
		Exchange:    exchange,
		Market:      market,
		Symbol:      symbol,
		Entries:     entries,
		Timestamp:   batchTimestamp,
		RecordCount: len(entries),
	}

	w.writeBatch(batch)
}

func (w *LiquidationWriter) writeBatch(batch liquidationBatch) {
	data, size, err := w.createParquet(batch)
	if err != nil {
		w.log.WithComponent("liq_writer").WithError(err).Error("failed to create parquet for liquidation batch")
		return
	}

	key := w.generateS3Key(batch)
	if err := w.upload(key, data); err != nil {
		w.log.WithComponent("liq_writer").WithError(err).WithFields(logger.Fields{
			"s3_key": key,
		}).Error("failed to upload liquidation batch")
		return
	}

	w.log.WithComponent("liq_writer").WithFields(logger.Fields{
		"s3_key":  key,
		"records": batch.RecordCount,
		"bytes":   size,
	}).Info("liquidation batch uploaded")
}

func (w *LiquidationWriter) createParquet(batch liquidationBatch) ([]byte, int64, error) {
	mf := newLiquidationMemFile()
	pw, err := writer.NewParquetWriter(mf, new(liquidationRecord), 1)
	if err != nil {
		return nil, 0, err
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, entry := range batch.Entries {
		eventTime := entry.EventTime
		if eventTime == 0 {
			eventTime = batch.Timestamp.UTC().UnixMilli()
		}
		rec := liquidationRecord{
			Exchange:    strings.ToLower(batch.Exchange),
			Market:      batch.Market,
			Symbol:      strings.ToUpper(batch.Symbol),
			EventTime:   eventTime,
			Side:        entry.Side,
			OrderType:   entry.OrderType,
			Price:       entry.Price,
			Quantity:    entry.Quantity,
			ReceivedTime: entry.ReceivedTime,
			Payload:     string(entry.RawPayload),
		}
		if err := pw.Write(rec); err != nil {
			return nil, 0, err
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, 0, err
	}

	data := mf.Bytes()
	return data, int64(len(data)), nil
}

func (w *LiquidationWriter) upload(key string, data []byte) error {
	if w.bucket == "" {
		return fmt.Errorf("s3 bucket not configured")
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	ctx := context.WithoutCancel(w.ctx)
	_, err := w.s3Client.PutObject(ctx, input)
	return err
}

func (w *LiquidationWriter) bufferKey(exchange, market, symbol string) string {
	exch := strings.ToLower(strings.TrimSpace(exchange))
	if exch == "" {
		exch = "unknown"
	}
	mkt := strings.ToLower(strings.TrimSpace(market))
	if mkt == "" {
		mkt = "liquidation"
	}
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	return strings.Join([]string{exch, mkt, sym}, liquidationKeySeparator)
}

func (w *LiquidationWriter) intervalForKey(key string) time.Duration {
	parts := strings.SplitN(key, liquidationKeySeparator, 2)
	if len(parts) == 0 {
		return defaultLiquidationFlush
	}
	if interval, ok := w.flushIntervals[parts[0]]; ok && interval > 0 {
		return interval
	}
	return defaultLiquidationFlush
}

func (w *LiquidationWriter) determineTickerInterval() time.Duration {
	min := time.Duration(0)
	for _, interval := range w.flushIntervals {
		if interval <= 0 {
			continue
		}
		if min == 0 || interval < min {
			min = interval
		}
	}
	if min == 0 {
		return time.Second
	}
	if min < time.Second {
		return min
	}
	return time.Second
}

func (w *LiquidationWriter) generateS3Key(batch liquidationBatch) string {
	timestamp := batch.Timestamp.UTC()

	var parts []string
	for _, key := range w.cfg.Writer.Partitioning.AdditionalKeys {
		switch key {
		case "exchange":
			parts = append(parts, fmt.Sprintf("exchange=%s", strings.ToLower(batch.Exchange)))
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

	ts := timestamp.Format("20060102150405")
	filename := fmt.Sprintf("%s_liq_%s_%s.parquet", strings.ToLower(batch.Exchange), strings.ToUpper(batch.Symbol), ts)
	return filepath.ToSlash(filepath.Join(append(parts, filename)...))
}

func buildLiquidationIntervals(cfg *appconfig.Config) map[string]time.Duration {
	intervals := map[string]time.Duration{}

	if d := cfg.Source.Binance.Future.Liquidation.FlushInterval; d > 0 {
		intervals["binance"] = d
	}
	if d := cfg.Source.Bybit.Future.Liquidation.FlushInterval; d > 0 {
		intervals["bybit"] = d
	}
	if d := cfg.Source.Kucoin.Future.Liquidation.FlushInterval; d > 0 {
		intervals["kucoin"] = d
	}
	if d := cfg.Source.Okx.Future.Liquidation.FlushInterval; d > 0 {
		intervals["okx"] = d
	}

	return intervals
}

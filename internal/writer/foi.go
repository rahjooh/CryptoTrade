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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/parquet"
	pqwriter "github.com/xitongsys/parquet-go/writer"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/metadata"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// FOI PARQUET RECORD //////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// BinanceFOIParquetRecord is the FOI schema stored on S3 for Binance.
// Separate schema per exchange as you requested.
type BinanceFOIParquetRecord struct {
	Exchange     string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Market       string  `parquet:"name=market, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol       string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventTimeMs  int64   `parquet:"name=event_time_ms, type=INT64"`
	OpenInterest float64 `parquet:"name=open_interest, type=DOUBLE"`
}

// BybitFOIParquetRecord is the dedicated Parquet schema used for Bybit FOI.
type BybitFOIParquetRecord struct {
	Exchange     string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Market       string  `parquet:"name=market, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol       string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Category     string  `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8"`
	Interval     string  `parquet:"name=interval, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventTimeMs  int64   `parquet:"name=event_time_ms, type=INT64"`
	OpenInterest float64 `parquet:"name=open_interest, type=DOUBLE"`
}

// OKXFOIParquetRecord defines the dedicated FOI schema used for OKX
// open-interest data on S3. Each exchange has its own schema so they
// can evolve independently.
type OKXFOIParquetRecord struct {
	Exchange    string  `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8"`
	Market      string  `parquet:"name=market, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol      string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"` // we derive symbol from instId family if needed
	InstID      string  `parquet:"name=inst_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	InstType    string  `parquet:"name=inst_type, type=BYTE_ARRAY, convertedtype=UTF8"`
	OI          float64 `parquet:"name=oi, type=DOUBLE"`
	OICcy       float64 `parquet:"name=oi_ccy, type=DOUBLE"`
	OIUsd       float64 `parquet:"name=oi_usd, type=DOUBLE"`
	EventTimeMs int64   `parquet:"name=event_time_ms, type=INT64"`
}

///////////////////////////////////////////////////////////////////////////////
//////////////////////////////// MEMORY WRITER ////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// memoryFileWriter implements ParquetFile interface for in-memory writing
type foiMemoryFileWriter struct {
	buffer *bytes.Buffer
}

func newFOIMemoryFileWriter() *foiMemoryFileWriter {
	return &foiMemoryFileWriter{
		buffer: &bytes.Buffer{},
	}
}

func (mfw *foiMemoryFileWriter) Create(name string) (source.ParquetFile, error) {
	return mfw, nil
}

func (mfw *foiMemoryFileWriter) Open(name string) (source.ParquetFile, error) {
	return mfw, nil
}

func (mfw *foiMemoryFileWriter) Seek(offset int64, whence int) (int64, error) {
	// For writing, we typically don't need seek; return current length.
	return int64(mfw.buffer.Len()), nil
}

func (mfw *foiMemoryFileWriter) Read(b []byte) (int, error) {
	return mfw.buffer.Read(b)
}

func (mfw *foiMemoryFileWriter) Write(b []byte) (int, error) {
	return mfw.buffer.Write(b)
}

func (mfw *foiMemoryFileWriter) Close() error {
	return nil
}

func (mfw *foiMemoryFileWriter) Bytes() []byte {
	return mfw.buffer.Bytes()
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// INTERNAL FOI TYPES //////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// internal batch for FOI writer (analog of BatchFOBSMessage but simpler)
type foiBatch struct {
	Exchange    string
	Market      string
	Symbol      string
	Entries     []models.NormFOI
	RecordCount int
	Timestamp   time.Time
}

// foiWriter buffers NormFOI messages and flushes them to S3 as Parquet.
type foiWriter struct {
	config      *appconfig.Config
	NormFOIch   <-chan models.NormFOI
	s3Client    *s3.Client
	ctx         context.Context
	wg          *sync.WaitGroup
	mu          sync.RWMutex
	running     bool
	log         *logger.Log
	buffer      map[string][]models.NormFOI
	flushTicker *time.Ticker
	metaGen     *metadata.Generator
}

// FOIWriter is the exported alias for foiWriter.
type FOIWriter = foiWriter

///////////////////////////////////////////////////////////////////////////////
/////////////////////////////// CONSTRUCTOR ///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

func newFOIWriter(cfg *appconfig.Config, NormFOIch <-chan models.NormFOI) (*foiWriter, error) {
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
		log.WithComponent("foi_s3_writer").WithError(err).Warn("failed to load AWS configuration")
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	creds, err := awsConfig.Credentials.Retrieve(ctx)
	if err != nil || !creds.HasKeys() {
		return nil, fmt.Errorf("aws credentials not found")
	}

	s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		if cfg.Storage.S3.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Storage.S3.Endpoint)
		}
		o.UsePathStyle = cfg.Storage.S3.PathStyle
	})

	metaDir, err := os.MkdirTemp("", "iceberg-foi")
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	gen := metadata.NewGenerator(
		metaDir,
		fmt.Sprintf("s3://%s", cfg.Storage.S3.Bucket),
		cfg.Storage.S3.Bucket,
		"", // table path or prefix if you use one
		cfg.Cryptoflow.Name,
		s3Client,
	)

	writer := &foiWriter{
		config:    cfg,
		NormFOIch: NormFOIch,
		s3Client:  s3Client,
		wg:        &sync.WaitGroup{},
		log:       log,
		metaGen:   gen,
	}

	log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"bucket":     cfg.Storage.S3.Bucket,
		"region":     cfg.Storage.S3.Region,
		"endpoint":   cfg.Storage.S3.Endpoint,
		"path_style": cfg.Storage.S3.PathStyle,
	}).Info("FOI s3 writer initialized")

	return writer, nil
}

// NewFOIWriter is the exported constructor.
func NewFOIWriter(cfg *appconfig.Config, NormFOIch <-chan models.NormFOI) (*FOIWriter, error) {
	return newFOIWriter(cfg, NormFOIch)
}

///////////////////////////////////////////////////////////////////////////////
//////////////////////// START / STOP / WORKERS ///////////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (w *foiWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("FOI s3 writer already running")
	}
	w.running = true
	w.ctx = ctx
	w.mu.Unlock()

	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting FOI s3 writer")

	w.buffer = make(map[string][]models.NormFOI)

	flushInterval := w.config.Writer.Buffer.OpenInterestInterval
	w.flushTicker = time.NewTicker(flushInterval)

	numWorkers := w.config.Writer.MaxWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}

	log.WithFields(logger.Fields{"workers": numWorkers, "flush_interval": flushInterval}).Info("starting FOI s3 writer workers")

	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}

	w.wg.Add(1)
	go w.flushWorker()

	log.Info("FOI s3 writer started successfully")
	return nil
}

func (w *foiWriter) Stop() {
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

	w.log.WithComponent("foi_s3_writer").Info("stopping FOI s3 writer")
	w.wg.Wait()
	w.log.WithComponent("foi_s3_writer").Info("FOI s3 writer stopped")
}

// /////////////////////////////////////////////////////////////////////////////
// ////////////////////////////// WORKERS //////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////
func (w *foiWriter) worker(workerID int) {
	defer w.wg.Done()

	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"worker_id": workerID,
		"worker":    "foi_s3_writer",
	})
	log.Info("starting FOI s3 writer worker")

	for {
		select {
		case <-w.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case env, ok := <-w.NormFOIch:
			if !ok {
				log.Info("foi.norm channel closed, worker stopping")
				return
			}
			if w.log.IsLevelEnabled(logrus.DebugLevel) {
				debugFields := logger.Fields{
					"exchange": env.Exchange,
				}
				switch env.Exchange {
				case models.ExchangeBinance:
					if env.Binance != nil {
						debugFields["symbol"] = env.Binance.Symbol
						debugFields["open_interest"] = env.Binance.OpenInterest
					}
				case models.ExchangeBybit:
					if env.Bybit != nil {
						debugFields["symbol"] = env.Bybit.Symbol
						debugFields["category"] = env.Bybit.Category
					}
				case models.ExchangeOKX:
					if env.OKX != nil {
						debugFields["inst_id"] = env.OKX.InstID
						debugFields["inst_type"] = env.OKX.InstType
					}
				}
				log.WithFields(debugFields).Debug("received normalized FOI message")
			}
			w.addNorm(env)
		}
	}
}

func (w *foiWriter) flushWorker() {
	defer w.wg.Done()

	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{"worker": "flush"})
	log.Info("starting FOI flush worker")

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

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// BUFFER MANAGEMENT ///////////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (w *foiWriter) bufferKey(exchange, market, symbol string) string {
	return fmt.Sprintf("%s|%s|%s", exchange, market, symbol)
}

// addNorm appends a NormFOI to the in-memory buffer keyed by
// (exchange, market=foi, symbol).
func (w *foiWriter) addNorm(env models.NormFOI) {
	const market = "foi"

	switch env.Exchange {
	case models.ExchangeBinance:
		b := env.Binance
		key := w.bufferKey(env.Exchange, market, b.Symbol)

		w.mu.Lock()
		w.buffer[key] = append(w.buffer[key], env)
		w.mu.Unlock()

	case models.ExchangeBybit:
		if env.Bybit == nil || env.Bybit.Symbol == "" {
			if w.log.IsLevelEnabled(logrus.DebugLevel) {
				w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
					"exchange": env.Exchange,
				}).Debug("skipping FOI message missing Bybit symbol")
			}
			return
		}
		symbol := env.Bybit.Symbol
		key := w.bufferKey(env.Exchange, market, symbol)
		w.mu.Lock()
		w.buffer[key] = append(w.buffer[key], env)
		w.mu.Unlock()

	case models.ExchangeOKX:
		if env.OKX == nil {
			if w.log.IsLevelEnabled(logrus.DebugLevel) {
				w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
					"exchange": env.Exchange,
				}).Debug("skipping FOI message missing OKX payload")
			}
			return
		}
		// by convention, use instId as "symbol" partition
		symbol := env.OKX.InstID
		key := w.bufferKey(env.Exchange, market, symbol)

		w.mu.Lock()
		w.buffer[key] = append(w.buffer[key], env)
		w.mu.Unlock()

	default:
		if w.log.IsLevelEnabled(logrus.DebugLevel) {
			w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
				"exchange": env.Exchange,
			}).Debug("received FOI entry for unsupported exchange; skipping")
		}
	}
}

func (w *foiWriter) flushBuffers(reason string) {
	w.mu.Lock()
	buffers := w.buffer
	w.buffer = make(map[string][]models.NormFOI)
	w.mu.Unlock()

	if len(buffers) == 0 {
		return
	}

	w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"flushed_buffers": len(buffers),
		"reason":          reason,
	}).Info("flushing FOI buffers")

	for key, entries := range buffers {
		if len(entries) == 0 {
			continue
		}
		parts := strings.SplitN(key, "|", 3)
		exchange, market, symbol := parts[0], parts[1], parts[2]

		// choose batch timestamp as last entry's Time
		ts := entries[len(entries)-1].Time
		batch := foiBatch{
			Exchange:    exchange,
			Market:      market,
			Symbol:      symbol,
			Entries:     entries,
			RecordCount: len(entries),
			Timestamp:   ts,
		}
		w.processBatch(batch)
	}
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// BATCH PROCESSING ////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (w *foiWriter) processBatch(batch foiBatch) {
	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"record_count": batch.RecordCount,
		"timestamp":    batch.Timestamp,
		"operation":    "process_batch",
	})

	if batch.RecordCount == 0 {
		log.Debug("FOI batch has no records, skipping")
		return
	}

	if w.log.IsLevelEnabled(logrus.DebugLevel) && len(batch.Entries) > 0 {
		entry := batch.Entries[0]
		symbol := ""
		switch entry.Exchange {
		case models.ExchangeBinance:
			if entry.Binance != nil {
				symbol = entry.Binance.Symbol
			}
		case models.ExchangeBybit:
			if entry.Bybit != nil {
				symbol = entry.Bybit.Symbol
			}
		case models.ExchangeOKX:
			if entry.OKX != nil {
				symbol = entry.OKX.InstID
			}
		}
		sample := logger.Fields{
			"sample_exchange": entry.Exchange,
			"sample_symbol":   symbol,
			"sample_time":     entry.Time,
		}
		log.WithFields(sample).Debug("sample FOI entry within batch")
	}

	s3Key := w.generateS3Key(batch)
	log = log.WithFields(logger.Fields{"s3_key": s3Key})
	log.Info("processing FOI batch")

	var (
		parquetData []byte
		fileSize    int64
		err         error
	)

	switch batch.Exchange {
	case models.ExchangeBinance:
		parquetData, fileSize, err = w.createBinanceParquetFile(batch)
	case models.ExchangeBybit:
		parquetData, fileSize, err = w.createBybitParquetFile(batch)
	case models.ExchangeOKX:
		parquetData, fileSize, err = w.createOKXParquetFile(batch)
	default:
		log.Warn("unsupported exchange for FOI writer, skipping batch")
		return
	}

	if err != nil {
		log.WithError(err).Error("failed to create FOI parquet file")
		return
	}

	if err := w.uploadToS3(s3Key, parquetData); err != nil {
		log.WithError(err).
			WithEnv("S3_BUCKET").
			WithFields(logger.Fields{"bucket": w.config.Storage.S3.Bucket, "s3_key": s3Key}).
			Error("failed to upload FOI parquet to S3")
		return
	}

	log.WithFields(logger.Fields{
		"file_size": fileSize,
	}).Info("FOI batch processed and uploaded successfully")

	df := metadata.DataFile{
		Path:        fmt.Sprintf("s3://%s/%s", w.config.Storage.S3.Bucket, s3Key),
		FileSize:    fileSize,
		RecordCount: int64(batch.RecordCount),
		Partition: map[string]any{
			"exchange": batch.Exchange,
			"market":   batch.Market,
			"symbol":   batch.Symbol,
			"date":     batch.Timestamp.Format("2006-01-02"),
		},
		Timestamp: batch.Timestamp,
	}

	if err := w.metaGen.AddFile(df); err != nil {
		log.WithError(err).Warn("failed to update FOI metadata")
	}
}

func (w *foiWriter) generateS3Key(batch foiBatch) string {
	timestamp := batch.Timestamp

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

	// Time-based partition path: e.g. date=2025-11-16
	timeFormat := w.config.Writer.Partitioning.TimeFormat
	timePath := strings.ReplaceAll(timeFormat, "{year}", fmt.Sprintf("%04d", timestamp.Year()))
	timePath = strings.ReplaceAll(timePath, "{month}", fmt.Sprintf("%02d", timestamp.Month()))
	timePath = strings.ReplaceAll(timePath, "{day}", fmt.Sprintf("%02d", timestamp.Day()))
	timePath = strings.ReplaceAll(timePath, "{hour}", fmt.Sprintf("%02d", timestamp.Hour()))
	parts = append(parts, timePath)

	// Filename: <exchange>_foi_<symbol>_<YYYYMMDDThhmmss>.parquet
	ts := timestamp.UTC().Format("20060102T150405")
	filename := fmt.Sprintf("%s_foi_%s_%s.parquet",
		batch.Exchange,
		batch.Symbol,
		ts,
	)

	key := filepath.Join(append(parts, filename)...)
	return filepath.ToSlash(key)
}

///////////////////////////////////////////////////////////////////////////////
//////////////////////////// PARQUET WRITERS //////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (w *foiWriter) createBinanceParquetFile(batch foiBatch) ([]byte, int64, error) {
	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"entries_count": batch.RecordCount,
		"exchange":      batch.Exchange,
		"symbol":        batch.Symbol,
		"operation":     "create_binance_parquet_file",
	})
	log.Info("creating Binance FOI parquet file")

	fw := newFOIMemoryFileWriter()
	pw, err := pqwriter.NewParquetWriter(fw, new(BinanceFOIParquetRecord), 4)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create Binance FOI parquet writer: %w", err)
	}

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

	for _, env := range batch.Entries {
		if env.Exchange != models.ExchangeBinance || env.Binance == nil {
			continue
		}
		b := env.Binance
		if b.Symbol == "" || b.EventTimeMs == 0 {
			continue
		}

		rec := BinanceFOIParquetRecord{
			Exchange:     batch.Exchange,
			Market:       batch.Market,
			Symbol:       b.Symbol,
			EventTimeMs:  b.EventTimeMs,
			OpenInterest: b.OpenInterest,
		}

		if err := pw.Write(rec); err != nil {
			pw.WriteStop()
			return nil, 0, fmt.Errorf("failed to write Binance FOI parquet record: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, 0, fmt.Errorf("failed to finalize Binance FOI parquet writing: %w", err)
	}

	data := fw.Bytes()
	log.WithFields(logger.Fields{
		"file_size":     len(data),
		"entries_count": batch.RecordCount,
		"compression":   w.config.Writer.Formats.Parquet.Compression,
	}).Info("Binance FOI parquet file created successfully")

	return data, int64(len(data)), nil
}

func (w *foiWriter) createBybitParquetFile(batch foiBatch) ([]byte, int64, error) {
	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"entries_count": batch.RecordCount,
		"exchange":      batch.Exchange,
		"symbol":        batch.Symbol,
		"operation":     "create_bybit_parquet_file",
	})
	log.Info("creating Bybit FOI parquet file")

	fw := newFOIMemoryFileWriter()
	pw, err := pqwriter.NewParquetWriter(fw, new(BybitFOIParquetRecord), 4)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create Bybit FOI parquet writer: %w", err)
	}

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

	for _, env := range batch.Entries {
		if env.Exchange != models.ExchangeBybit || env.Bybit == nil {
			continue
		}
		b := env.Bybit
		if b.Symbol == "" || b.EventTimeMs == 0 {
			continue
		}

		rec := BybitFOIParquetRecord{
			Exchange:     batch.Exchange,
			Market:       batch.Market,
			Symbol:       b.Symbol,
			Category:     b.Category,
			Interval:     b.Interval,
			EventTimeMs:  b.EventTimeMs,
			OpenInterest: b.OpenInterest,
		}

		if err := pw.Write(rec); err != nil {
			pw.WriteStop()
			return nil, 0, fmt.Errorf("failed to write Bybit FOI parquet record: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, 0, fmt.Errorf("failed to finalize Bybit FOI parquet writing: %w", err)
	}

	data := fw.Bytes()
	log.WithFields(logger.Fields{
		"file_size":     len(data),
		"entries_count": batch.RecordCount,
		"compression":   w.config.Writer.Formats.Parquet.Compression,
	}).Info("Bybit FOI parquet file created successfully")

	return data, int64(len(data)), nil
}

func (w *foiWriter) createOKXParquetFile(batch foiBatch) ([]byte, int64, error) {
	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"entries_count": batch.RecordCount,
		"exchange":      batch.Exchange,
		"symbol":        batch.Symbol,
		"operation":     "create_okx_parquet_file",
	})
	log.Info("creating OKX FOI parquet file")

	fw := newFOIMemoryFileWriter()
	pw, err := pqwriter.NewParquetWriter(fw, new(OKXFOIParquetRecord), 4)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create OKX FOI parquet writer: %w", err)
	}

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

	for _, env := range batch.Entries {
		if env.Exchange != models.ExchangeOKX || env.OKX == nil {
			continue
		}
		o := env.OKX
		if o.InstID == "" || o.EventTimeMs == 0 {
			continue
		}

		rec := OKXFOIParquetRecord{
			Exchange:    batch.Exchange,
			Market:      batch.Market,
			Symbol:      batch.Symbol,
			InstID:      o.InstID,
			InstType:    o.InstType,
			OI:          o.OI,
			OICcy:       o.OICcy,
			OIUsd:       o.OIUsd,
			EventTimeMs: o.EventTimeMs,
		}

		if err := pw.Write(rec); err != nil {
			pw.WriteStop()
			return nil, 0, fmt.Errorf("failed to write OKX FOI parquet record: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, 0, fmt.Errorf("failed to finalize OKX FOI parquet writing: %w", err)
	}

	data := fw.Bytes()
	log.WithFields(logger.Fields{
		"file_size":     len(data),
		"entries_count": batch.RecordCount,
		"compression":   w.config.Writer.Formats.Parquet.Compression,
	}).Info("OKX FOI parquet file created successfully")

	return data, int64(len(data)), nil
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////////// S3 UPLOAD /////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (w *foiWriter) uploadToS3(key string, data []byte) error {
	log := w.log.WithComponent("foi_s3_writer").WithFields(logger.Fields{
		"operation": "upload_to_s3",
		"data_size": len(data),
	})
	log.Info("uploading FOI parquet to S3")

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

	ctx := context.WithoutCancel(w.ctx)
	_, err := w.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload FOI parquet to S3 bucket %s: %w", w.config.Storage.S3.Bucket, err)
	}

	log.Info("successfully uploaded FOI parquet to S3")
	return nil
}

// Exposed methods for external packages.
func (w *FOIWriter) StartWriter(ctx context.Context) error { return w.Start(ctx) }
func (w *FOIWriter) StopWriter()                           { w.Stop() }

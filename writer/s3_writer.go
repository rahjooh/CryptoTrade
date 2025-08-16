package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/google/uuid"

	appconfig "cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// S3Writer buffers order book batches and periodically persists them as
// JSON objects to the S3 bucket backing an S3 Table. The official S3
// Tables WriteRows API is still unavailable in the AWS SDK, so this
// implementation uploads batches via standard S3 PutObject calls.
type S3Writer struct {
	config        *appconfig.Config
	flattenedChan <-chan models.FlattenedOrderbookBatch
	s3Table       *s3tables.Client
	s3Client      *s3.Client
	bucket        string
	prefix        string
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
	buffer        map[string][]models.FlattenedOrderbookEntry
	flushTicker   *time.Ticker

	// Metrics
	batchesWritten int64
	rowsWritten    int64
	errorsCount    int64
}

// NewS3Writer creates a new S3Writer instance. It configures the AWS SDK
// and initializes the S3Tables client used for writing rows.
func NewS3Writer(cfg *appconfig.Config, flattenedChan <-chan models.FlattenedOrderbookBatch) (*S3Writer, error) {
	log := logger.GetLogger()
	ctx := context.Background()

	// Configure AWS options
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

	s3TableClient := s3tables.NewFromConfig(awsConfig)
	s3Client := s3.NewFromConfig(awsConfig)

	bucket, prefix, err := parseBucketAndPrefix(cfg.Storage.S3.TableARN)
	if err != nil {
		return nil, fmt.Errorf("invalid table arn: %w", err)
	}

	writer := &S3Writer{
		config:        cfg,
		flattenedChan: flattenedChan,
		s3Table:       s3TableClient,
		s3Client:      s3Client,
		bucket:        bucket,
		prefix:        prefix,
		wg:            &sync.WaitGroup{},
		log:           log,
	}

	log.WithComponent("s3_writer").WithFields(logger.Fields{
		"region":    cfg.Storage.S3.Region,
		"table_arn": cfg.Storage.S3.TableARN,
	}).Info("s3 writer initialized")

	return writer, nil
}

// Start launches the worker and flush goroutines. Batches are buffered and
// flushed at the interval specified in the configuration.
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

	// Launch worker goroutines to receive batches concurrently.
	numWorkers := w.config.Writer.MaxWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}
	log.WithFields(logger.Fields{"workers": numWorkers}).Info("starting s3 writer workers")

	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}

	// Launch flush worker to periodically write buffered rows.
	w.wg.Add(1)
	go w.flushWorker()

	// Emit metrics periodically.
	go w.metricsReporter(ctx)

	log.Info("s3 writer started successfully")
	return nil
}

// Stop waits for all workers to finish and stops the flush ticker.
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

// worker consumes batches from the flattened channel and buffers their
// entries for later flushing.
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
			logger.LogDataFlowEntry(log, "flattened_channel", "s3_table_buffer", batch.RecordCount, "rows")
		}
	}
}

// addBatch appends the entries of a batch into a per-symbol buffer so that
// multiple batches for the same symbol can be flushed together.
func (w *S3Writer) addBatch(batch models.FlattenedOrderbookBatch) {
	key := w.bufferKey(batch.Exchange, batch.Market, batch.Symbol)
	w.mu.Lock()
	w.buffer[key] = append(w.buffer[key], batch.Entries...)
	w.mu.Unlock()
}

// bufferKey generates the map key used for buffering entries by symbol.
func (w *S3Writer) bufferKey(exchange, market, symbol string) string {
	return fmt.Sprintf("%s|%s|%s", exchange, market, symbol)
}

// flushWorker periodically flushes all buffered entries to the S3 table.
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

// flushBuffers swaps the current buffer with a new one and processes each
// batch group sequentially.
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

// processBatch writes a single batch of entries to the S3 table.
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

	if err := w.writeRowsToS3Table(batch); err != nil {
		atomic.AddInt64(&w.errorsCount, 1)
		log.WithError(err).Error("failed to write rows to S3 table")
		return
	}

	atomic.AddInt64(&w.rowsWritten, int64(batch.RecordCount))
	log.Info("batch written to S3 table successfully")
	logger.LogDataFlowEntry(log, "flattened_channel", "s3_table", batch.RecordCount, "rows")
	w.log.LogMetric("s3_writer", "rows_written", int64(batch.RecordCount), "counter", logger.Fields{
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"record_count": batch.RecordCount,
	})
}


// writeRowsToS3Table writes a batch to the underlying S3 bucket as a JSON
// document. Each batch becomes an object whose key encodes exchange, market,
// symbol and timestamp, allowing downstream tools to process the files or load
// them into an S3 Table.
func (w *S3Writer) writeRowsToS3Table(batch models.FlattenedOrderbookBatch) error {
	if w.s3Client == nil {
		return fmt.Errorf("s3 client not initialized")
	}

	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	ts := batch.Timestamp.UTC().Format("2006-01-02T15-04-05.000000000Z07:00")
	key := path.Join(w.prefix, batch.Exchange, batch.Market, batch.Symbol, ts+"_"+batch.BatchID+".json")

	_, err = w.s3Client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	return nil
}

// parseBucketAndPrefix extracts the S3 bucket name and prefix from an S3 Table
// ARN of the form:
// arn:aws:s3tables:region:account:tablebucket/{bucket}/table/{namespace}/{table}
func parseBucketAndPrefix(tableARN string) (bucket, prefix string, err error) {
	parsed, err := awsarn.Parse(tableARN)
	if err != nil {
		return "", "", err
	}

	parts := strings.Split(parsed.Resource, "/")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid table arn resource: %s", parsed.Resource)
	}

	bucket = parts[1]
	if len(parts) > 2 {
		prefix = strings.Join(parts[2:], "/")
	}
	return bucket, prefix, nil
}

// metricsReporter periodically publishes internal metrics for observability.
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

// reportMetrics emits aggregated metrics about writer performance.
func (w *S3Writer) reportMetrics() {
	batchesWritten := atomic.LoadInt64(&w.batchesWritten)
	rowsWritten := atomic.LoadInt64(&w.rowsWritten)
	errorsCount := atomic.LoadInt64(&w.errorsCount)

	errorRate := float64(0)
	if batchesWritten+errorsCount > 0 {
		errorRate = float64(errorsCount) / float64(batchesWritten+errorsCount)
	}

	log := w.log.WithComponent("s3_writer")
	log.LogMetric("s3_writer", "batches_written", batchesWritten, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "rows_written", rowsWritten, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "errors_count", errorsCount, "counter", logger.Fields{})
	log.LogMetric("s3_writer", "error_rate", errorRate, "gauge", logger.Fields{})

	log.WithFields(logger.Fields{
		"batches_written": batchesWritten,
		"rows_written":    rowsWritten,
		"errors_count":    errorsCount,
		"error_rate":      errorRate,
	}).Info("s3 writer metrics")
}

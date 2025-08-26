package writer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

const maxS3RequestBytes = 10 * 1024 * 1024 // 10MB limit for S3 Tables API
// S3Writer buffers order book batches and writes them directly to an
// Amazon S3 Table using the Iceberg REST endpoint. Batches are sent over
// the S3 Tables API with SigV4 authentication, avoiding Glue or Athena.
type S3Writer struct {
	config        *appconfig.Config
	flattenedChan <-chan models.FlattenedOrderbookBatch
	s3Table       *s3tables.Client
	bucket        string
	namespace     string
	table         string
	awsConfig     aws.Config
	httpClient    *http.Client
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log

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

	bucket, err := parseTableBucketARN(cfg.Storage.S3.TableBucketARN)
	if err != nil {
		return nil, fmt.Errorf("invalid table bucket arn: %w", err)
	}
	namespace := cfg.Storage.S3.Namespace
	table := cfg.Storage.S3.TableName
	if namespace == "" || table == "" {
		return nil, fmt.Errorf("missing namespace or table name")
	}

	writer := &S3Writer{
		config:        cfg,
		flattenedChan: flattenedChan,
		s3Table:       s3TableClient,
		bucket:        bucket,
		namespace:     namespace,
		table:         table,
		awsConfig:     awsConfig,
		httpClient:    &http.Client{},
		wg:            &sync.WaitGroup{},
		log:           log,
	}

	if err := writer.ensurePrerequisites(ctx); err != nil {
		return nil, fmt.Errorf("ensure prerequisites: %w", err)
	}

	log.WithComponent("s3_writer").WithFields(logger.Fields{
		"region":    cfg.Storage.S3.Region,
		"table_arn": cfg.Storage.S3.TableARN,
	}).Debug("s3 writer initialized")

	return writer, nil
}

// Start launches worker goroutines that write each incoming batch
// immediately to the S3 table.
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
	log.Debug("starting s3 writer")

	// Launch worker goroutines to receive batches concurrently.
	numWorkers := w.config.Writer.MaxWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}
	log.WithFields(logger.Fields{"workers": numWorkers}).Debug("starting s3 writer workers")

	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}

	// Emit metrics periodically.
	go w.metricsReporter(ctx)

	log.Debug("s3 writer started successfully")
	return nil
}

// Stop waits for all workers to finish
func (w *S3Writer) Stop() {
	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	w.log.WithComponent("s3_writer").Debug("stopping s3 writer")
	w.wg.Wait()
	w.log.WithComponent("s3_writer").Debug("s3 writer stopped")
}

// worker consumes batches from the flattened channel and writes them to
// the S3 table.
func (w *S3Writer) worker(workerID int) {
	defer w.wg.Done()

	log := w.log.WithComponent("s3_writer").WithFields(logger.Fields{
		"worker_id": workerID,
		"worker":    "s3_writer",
	})

	log.Debug("starting s3 writer worker")

	for {
		select {
		case <-w.ctx.Done():
			log.Debug("worker stopped due to context cancellation")
			return
		case batch, ok := <-w.flattenedChan:
			if !ok {
				log.Debug("flattened channel closed, worker stopping")
				return
			}
			w.processBatch(batch)
			atomic.AddInt64(&w.batchesWritten, 1)
			logger.LogDataFlowEntry(log, "flattened_channel", "s3_table", batch.RecordCount, "rows")
		}
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

	log.Debug("processing batch")

	if batch.RecordCount == 0 {
		log.Debug("batch has no records, skipping")
		return
	}

	batches, err := w.splitBatch(batch, maxS3RequestBytes)
	if err != nil {
		atomic.AddInt64(&w.errorsCount, 1)
		log.WithError(err).Error("failed to split batch")
		return
	}
	for _, sb := range batches {
		size, err := w.writeRowsToS3Table(sb)
		if err != nil {
			atomic.AddInt64(&w.errorsCount, 1)
			log.WithFields(logger.Fields{"sub_batch_id": sb.BatchID}).WithError(err).Error("failed to write rows to S3 table")
			return
		}

		atomic.AddInt64(&w.rowsWritten, int64(sb.RecordCount))
		log.WithFields(logger.Fields{
			"sub_batch_id":     sb.BatchID,
			"batch_size_bytes": size,
			"record_count":     sb.RecordCount,
		}).Info("batch written to S3 table")
		logger.LogDataFlowEntry(log, "flattened_channel", "s3_table", sb.RecordCount, "rows")
		w.log.LogMetric("s3_writer", "rows_written", int64(sb.RecordCount), "counter", logger.Fields{
			"exchange":     sb.Exchange,
			"symbol":       sb.Symbol,
			"record_count": sb.RecordCount,
		})
	}
}

// splitBatch recursively divides a batch until each piece is within the maxBytes limit.
func (w *S3Writer) splitBatch(batch models.FlattenedOrderbookBatch, maxBytes int) ([]models.FlattenedOrderbookBatch, error) {
	data, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("marshal batch: %w", err)
	}
	if len(data) <= maxBytes {
		return []models.FlattenedOrderbookBatch{batch}, nil
	}
	if len(batch.Entries) <= 1 {
		return nil, fmt.Errorf("batch too large and cannot be split further")
	}

	mid := len(batch.Entries) / 2

	left := batch
	left.BatchID = uuid.New().String()
	left.Entries = batch.Entries[:mid]
	left.RecordCount = len(left.Entries)

	right := batch
	right.BatchID = uuid.New().String()
	right.Entries = batch.Entries[mid:]
	right.RecordCount = len(right.Entries)

	lb, err := w.splitBatch(left, maxBytes)
	if err != nil {
		return nil, err
	}
	rb, err := w.splitBatch(right, maxBytes)
	if err != nil {
		return nil, err
	}

	return append(lb, rb...), nil
}

// writeRowsToS3Table sends the batch directly to the S3 Tables Iceberg REST
// endpoint. The request is signed with SigV4 using the "s3tables" signing name
// and authenticated with the configured AWS credentials.
func (w *S3Writer) writeRowsToS3Table(batch models.FlattenedOrderbookBatch) (int, error) {
	data, err := json.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("marshal batch: %w", err)
	}

	baseEndpoint := w.config.Storage.S3.Endpoint
	if baseEndpoint == "" {
		baseEndpoint = fmt.Sprintf("https://s3tables.%s.amazonaws.com/iceberg/v1", w.awsConfig.Region)
	}
	endpoint := fmt.Sprintf("%s/namespaces/%s/tables/%s/rows", strings.TrimSuffix(baseEndpoint, "/"), w.namespace, w.table)

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	creds, err := w.awsConfig.Credentials.Retrieve(w.ctx)
	if err != nil {
		return 0, fmt.Errorf("retrieve credentials: %w", err)
	}

	hash := sha256.Sum256(data)
	signer := v4.NewSigner()
	if err := signer.SignHTTP(w.ctx, creds, req, hex.EncodeToString(hash[:]), "s3tables", w.awsConfig.Region, time.Now()); err != nil {
		return 0, fmt.Errorf("sign request: %w", err)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("s3 tables write failed: %s", string(body))
	}

	return len(data), nil
}

// parseTableBucketARN extracts the bucket name from an S3 Table Bucket ARN.
func parseTableBucketARN(bucketARN string) (string, error) {
	parsed, err := awsarn.Parse(bucketARN)
	if err != nil {
		return "", err
	}

	parts := strings.Split(parsed.Resource, "/")
	if len(parts) != 2 || parts[0] != "bucket" {
		return "", fmt.Errorf("invalid table bucket arn resource: %s", parsed.Resource)
	}

	return parts[1], nil
}

// ensurePrerequisites creates the table bucket, namespace, and table if they do
// not already exist.
func (w *S3Writer) ensurePrerequisites(ctx context.Context) error {
	// Ensure table bucket exists.
	if _, err := w.s3Table.GetTableBucket(ctx, &s3tables.GetTableBucketInput{
		TableBucketARN: aws.String(w.config.Storage.S3.TableBucketARN),
	}); err != nil {
		if isNotFound(err) {
			if _, err := w.s3Table.CreateTableBucket(ctx, &s3tables.CreateTableBucketInput{
				Name: aws.String(w.bucket),
			}); err != nil && !isConflict(err) {
				return fmt.Errorf("create table bucket: %w", err)
			}
		} else {
			return fmt.Errorf("get table bucket: %w", err)
		}
	}

	// Ensure namespace exists.
	if _, err := w.s3Table.GetNamespace(ctx, &s3tables.GetNamespaceInput{
		Namespace:      aws.String(w.namespace),
		TableBucketARN: aws.String(w.config.Storage.S3.TableBucketARN),
	}); err != nil {
		if isNotFound(err) {
			if _, err := w.s3Table.CreateNamespace(ctx, &s3tables.CreateNamespaceInput{
				Namespace:      []string{w.namespace},
				TableBucketARN: aws.String(w.config.Storage.S3.TableBucketARN),
			}); err != nil && !isConflict(err) {
				return fmt.Errorf("create namespace: %w", err)
			}
		} else {
			return fmt.Errorf("get namespace: %w", err)
		}
	}

	// Ensure table exists.
	if _, err := w.s3Table.GetTable(ctx, &s3tables.GetTableInput{
		Name:           aws.String(w.table),
		Namespace:      aws.String(w.namespace),
		TableBucketARN: aws.String(w.config.Storage.S3.TableBucketARN),
	}); err != nil {
		if isNotFound(err) {
			if _, err := w.s3Table.CreateTable(ctx, &s3tables.CreateTableInput{
				Format:         s3types.OpenTableFormatIceberg,
				Name:           aws.String(w.table),
				Namespace:      aws.String(w.namespace),
				TableBucketARN: aws.String(w.config.Storage.S3.TableBucketARN),
			}); err != nil && !isConflict(err) {
				return fmt.Errorf("create table: %w", err)
			}
		} else {
			return fmt.Errorf("get table: %w", err)
		}
	}

	return nil
}

func isNotFound(err error) bool {
	var nfe *s3types.NotFoundException
	return errors.As(err, &nfe)
}

func isConflict(err error) bool {
	var ce *s3types.ConflictException
	return errors.As(err, &ce)
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
	}).Debug("s3 writer metrics")
}

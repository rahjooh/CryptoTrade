package writer

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

// LiquidationWriter persists raw liquidation messages to S3 using a worker pool.
type LiquidationWriter struct {
	cfg        *appconfig.Config
	rawChan    <-chan models.RawLiquidationMessage
	s3Client   *s3.Client
	log        *logger.Log
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	running    bool
	mu         sync.Mutex
	workerPool chan struct{}
}

// NewLiquidationWriter initializes the writer using S3 credentials from config.
func NewLiquidationWriter(cfg *appconfig.Config, rawChan <-chan models.RawLiquidationMessage) (*LiquidationWriter, error) {
	log := logger.GetLogger()
	if !cfg.Storage.S3.Enabled {
		return nil, fmt.Errorf("s3 storage is disabled")
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

	concurrency := cfg.Storage.S3.UploadConcurrency
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	return &LiquidationWriter{
		cfg:        cfg,
		rawChan:    rawChan,
		s3Client:   s3Client,
		log:        log,
		wg:         &sync.WaitGroup{},
		workerPool: make(chan struct{}, concurrency),
	}, nil
}

// Start launches the dispatcher and worker routines.
func (w *LiquidationWriter) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("liquidation writer already running")
	}
	w.running = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.mu.Unlock()

	w.log.WithComponent("liq_writer").WithFields(logger.Fields{"workers": cap(w.workerPool)}).Info("starting liquidation writer")

	w.wg.Add(1)
	go w.dispatch()
	return nil
}

// Stop waits for workers and flushes pending uploads.
func (w *LiquidationWriter) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	cancel := w.cancel
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	w.wg.Wait()
	w.log.WithComponent("liq_writer").Info("liquidation writer stopped")
}

func (w *LiquidationWriter) dispatch() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case msg, ok := <-w.rawChan:
			if !ok {
				return
			}
			w.enqueue(msg)
		}
	}
}

func (w *LiquidationWriter) enqueue(msg models.RawLiquidationMessage) {
	select {
	case w.workerPool <- struct{}{}:
	case <-w.ctx.Done():
		return
	}

	w.wg.Add(1)
	go func(m models.RawLiquidationMessage) {
		defer w.wg.Done()
		defer func() { <-w.workerPool }()
		if err := w.upload(m); err != nil {
			w.log.WithComponent("liq_writer").WithError(err).WithFields(logger.Fields{
				"exchange": m.Exchange,
				"symbol":   m.Symbol,
			}).Warn("failed to upload liquidation message")
		}
	}(msg)
}

func (w *LiquidationWriter) upload(msg models.RawLiquidationMessage) error {
	if msg.Exchange == "" || msg.Symbol == "" {
		return fmt.Errorf("invalid liquidation message metadata")
	}

	bucket := w.cfg.Storage.S3.Bucket
	if bucket == "" {
		return fmt.Errorf("s3 bucket not configured")
	}

	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	date := ts.UTC().Format("2006-01-02")
	symbol := strings.ToUpper(msg.Symbol)
	exchange := strings.ToLower(msg.Exchange)

	key := filepath.Join(
		fmt.Sprintf("exchange=%s", exchange),
		"market=liquidation",
		fmt.Sprintf("symbol=%s", symbol),
		fmt.Sprintf("date=%s", date),
		fmt.Sprintf("%s.json", uuid.NewString()),
	)

	payload := bytes.NewReader(msg.Data)
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        payload,
		ContentType: aws.String("application/json"),
	}

	_, err := w.s3Client.PutObject(w.ctx, input)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	w.log.WithComponent("liq_writer").WithFields(logger.Fields{
		"key":      key,
		"exchange": exchange,
		"symbol":   symbol,
	}).Debug("uploaded liquidation payload")
	return nil
}

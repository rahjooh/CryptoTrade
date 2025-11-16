package writer

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go/parquet"
	pqwriter "github.com/xitongsys/parquet-go/writer"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/models"
	"cryptoflow/logger"
)

type S3WriterConfig struct {
	Bucket string
	Prefix string // e.g. "liq/"
}

type S3ParquetWriter struct {
	cfg    S3WriterConfig
	client *s3.Client
}

func NewS3ParquetWriter(ctx context.Context, cfg S3WriterConfig) (*S3ParquetWriter, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg)

	return &S3ParquetWriter{
		cfg:    cfg,
		client: client,
	}, nil
}

// Run consumes normalized envelopes and writes Parquet files to S3.
func (w *S3ParquetWriter) Run(ctx context.Context, liqNormCh <-chan models.NormalizedLiquidation) {
	const (
		maxBatchSize = 2000
		maxBatchAge  = 15 * time.Second
	)

	batch := make([]models.NormalizedLiquidation, 0, maxBatchSize)
	lastFlush := time.Now()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.writeBatchesByExchange(ctx, batch); err != nil {
			log.Printf("[s3-parquet-writer] write batches error: %v", err)
		}
		batch = batch[:0]
		lastFlush = time.Now()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[s3-parquet-writer] ctx canceled, flushing final batch")
			flush()
			return

		case ev, ok := <-liqNormCh:
			if !ok {
				log.Printf("[s3-parquet-writer] liq.norm closed, flushing final batch")
				flush()
				return
			}
			batch = append(batch, ev)
			if len(batch) >= maxBatchSize {
				flush()
			}

		case <-ticker.C:
			if time.Since(lastFlush) >= maxBatchAge {
				flush()
			}
		}
	}
}

// group by exchange and write one or more Parquet objects per exchange
func (w *S3ParquetWriter) writeBatchesByExchange(ctx context.Context, events []models.NormalizedLiquidation) error {
	byEx := make(map[string][]models.NormalizedLiquidation)
	for _, ev := range events {
		if ev.Exchange == "" {
			continue
		}
		byEx[ev.Exchange] = append(byEx[ev.Exchange], ev)
	}

	for ex, group := range byEx {
		if len(group) == 0 {
			continue
		}
		var err error
		switch ex {
		case models.ExchangeBinance:
			err = w.writeBinanceBatch(ctx, group)
		case models.ExchangeBybit:
			err = w.writeBybitBatch(ctx, group)
		case models.ExchangeOKX:
			err = w.writeOKXBatch(ctx, group)
		default:
			// unknown exchange; ignore
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// BINANCE SCHEMA & WRITER
////////////////////////////////////////////////////////////////////////////////

// parquet schema just for Binance liquidation normalized data
type binanceParquetRow struct {
	Exchange string `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Symbol   string `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Side     string `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	PositionSide string `parquet:"name=position_side, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OrderType    string `parquet:"name=order_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	TimeMillis int64 `parquet:"name=time_millis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`

	Quantity  float64 `parquet:"name=quantity, type=DOUBLE"`
	Price     float64 `parquet:"name=price, type=DOUBLE"`
	AvgPrice  float64 `parquet:"name=avg_price, type=DOUBLE"`
	LastQty   float64 `parquet:"name=last_qty, type=DOUBLE"`
	LastPrice float64 `parquet:"name=last_price, type=DOUBLE"`

	TradeID int64 `parquet:"name=trade_id, type=INT64"`

	IsMaker      bool    `parquet:"name=is_maker, type=BOOLEAN"`
	IsReduceOnly bool    `parquet:"name=is_reduce_only, type=BOOLEAN"`
	WorkingType  string  `parquet:"name=working_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OriginalType string  `parquet:"name=original_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CloseAll     bool    `parquet:"name=close_all, type=BOOLEAN"`
	RealizedPnl  float64 `parquet:"name=realized_pnl, type=DOUBLE"`
}

func (w *S3ParquetWriter) writeBinanceBatch(ctx context.Context, events []models.NormalizedLiquidation) error {
	// filter only envelopes that actually have Binance data
	rows := make([]binanceParquetRow, 0, len(events))
	for _, env := range events {
		if env.Binance == nil {
			continue
		}
		b := env.Binance
		rows = append(rows, binanceParquetRow{
			Exchange:     models.ExchangeBinance,
			Symbol:       b.Symbol,
			Side:         b.Side,
			PositionSide: b.PositionSide,
			OrderType:    b.OrderType,
			TimeMillis:   env.Time.UTC().UnixNano() / int64(time.Millisecond),
			Quantity:     b.Quantity,
			Price:        b.Price,
			AvgPrice:     b.AvgPrice,
			LastQty:      b.LastQty,
			LastPrice:    b.LastPrice,
			TradeID:      b.TradeID,
			IsMaker:      b.IsMaker,
			IsReduceOnly: b.IsReduceOnly,
			WorkingType:  b.WorkingType,
			OriginalType: b.OriginalType,
			CloseAll:     b.CloseAll,
			RealizedPnl:  b.RealizedPnl,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	key := w.liq_s3Key(events)
	var buf bytes.Buffer
	pw, err := pqwriter.NewParquetWriterFromWriter(&buf, new(binanceParquetRow), 4)
	if err != nil {
		return fmt.Errorf("binance: create parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("binance: parquet write row: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("binance: parquet write stop: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}
	if _, err := w.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("binance: s3 put object: %w", err)
	}

	log.Printf("[s3-parquet-writer] binance wrote %d rows -> s3://%s/%s", len(rows), w.cfg.Bucket, key)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// BYBIT SCHEMA & WRITER
////////////////////////////////////////////////////////////////////////////////

type bybitParquetRow struct {
	Exchange string `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Symbol   string `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Side     string `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	TimeMillis int64   `parquet:"name=time_millis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Quantity   float64 `parquet:"name=quantity, type=DOUBLE"`
	Price      float64 `parquet:"name=price, type=DOUBLE"`
}

func (w *S3ParquetWriter) writeBybitBatch(ctx context.Context, events []models.NormalizedLiquidation) error {
	rows := make([]bybitParquetRow, 0, len(events))
	for _, env := range events {
		if env.Bybit == nil {
			continue
		}
		b := env.Bybit
		rows = append(rows, bybitParquetRow{
			Exchange:   models.ExchangeBybit,
			Symbol:     b.Symbol,
			Side:       b.Side,
			TimeMillis: env.Time.UTC().UnixNano() / int64(time.Millisecond),
			Quantity:   b.Quantity,
			Price:      b.Price,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	key := w.liq_s3Key(events)

	var buf bytes.Buffer
	pw, err := pqwriter.NewParquetWriterFromWriter(&buf, new(bybitParquetRow), 4)
	if err != nil {
		return fmt.Errorf("bybit: create parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("bybit: parquet write row: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("bybit: parquet write stop: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}
	if _, err := w.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("bybit: s3 put object: %w", err)
	}

	log.Printf("[s3-parquet-writer] bybit wrote %d rows -> s3://%s/%s", len(rows), w.cfg.Bucket, key)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// OKX SCHEMA & WRITER
////////////////////////////////////////////////////////////////////////////////

type okxParquetRow struct {
	Exchange string `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Symbol   string `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Side     string `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	PositionSide string `parquet:"name=position_side, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	TimeMillis int64   `parquet:"name=time_millis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Quantity   float64 `parquet:"name=quantity, type=DOUBLE"`
	Price      float64 `parquet:"name=price, type=DOUBLE"`
}

func (w *S3ParquetWriter) writeOKXBatch(ctx context.Context, events []models.NormalizedLiquidation) error {
	rows := make([]okxParquetRow, 0, len(events))
	for _, env := range events {
		if env.OKX == nil {
			continue
		}
		o := env.OKX
		rows = append(rows, okxParquetRow{
			Exchange:     models.ExchangeOKX,
			Symbol:       o.Symbol,
			Side:         o.Side,
			PositionSide: o.PositionSide,
			TimeMillis:   env.Time.UTC().UnixNano() / int64(time.Millisecond),
			Quantity:     o.Quantity,
			Price:        o.Price,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	key := w.liq_s3Key(events)

	var buf bytes.Buffer
	pw, err := pqwriter.NewParquetWriterFromWriter(&buf, new(okxParquetRow), 4)
	if err != nil {
		return fmt.Errorf("okx: create parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("okx: parquet write row: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("okx: parquet write stop: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}
	if _, err := w.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("okx: s3 put object: %w", err)
	}

	log.Printf("[s3-parquet-writer] okx wrote %d rows -> s3://%s/%s", len(rows), w.cfg.Bucket, key)
	return nil
}

type LiquidationWriter struct {
	cfg      *appconfig.Config
	normChan <-chan models.NormalizedLiquidation
	s3       *S3ParquetWriter
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
	log      *logger.Log
}

func NewLiquidationWriter(cfg *appconfig.Config, norm <-chan models.NormalizedLiquidation) (*LiquidationWriter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	if !cfg.Storage.S3.Enabled {
		return nil, fmt.Errorf("s3 storage disabled")
	}
	if norm == nil {
		return nil, fmt.Errorf("nil normalized channel")
	}

	prefix := "liq/"
	if cfg.Cryptoflow.Name != "" {
		prefix = fmt.Sprintf("%s/liquidation/", cfg.Cryptoflow.Name)
	}

	s3Writer, err := NewS3ParquetWriter(context.Background(), S3WriterConfig{
		Bucket: cfg.Storage.S3.Bucket,
		Prefix: prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("create liquidation s3 writer: %w", err)
	}

	return &LiquidationWriter{
		cfg:      cfg,
		normChan: norm,
		s3:       s3Writer,
		log:      logger.GetLogger(),
	}, nil
}

func (w *LiquidationWriter) Start(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("nil liquidation writer")
	}

	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("liquidation writer already running")
	}
	w.running = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.mu.Unlock()

	w.log.WithComponent("liquidation_writer").Info("starting liquidation writer")
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.s3.Run(w.ctx, w.normChan)
	}()
	return nil
}

func (w *LiquidationWriter) Stop() {
	if w == nil {
		return
	}

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
	w.log.WithComponent("liquidation_writer").Info("liquidation writer stopped")
}

// liqS3Key builds a key like:
//
//	<prefix>exchange=binance/market=liquidation/symbol=BTCUSDC/date=2025-09-06/binance_liq_BTCUSDC_20250906T131500.parquet
func (w *S3ParquetWriter) liq_s3Key(events []models.NormalizedLiquidation) string {
	if len(events) == 0 {
		// Defensive fallback
		return "exchange=unknown/market=liquidation/symbol=unknown/date=1970-01-01/unknown_liq_unknown_19700101T000000.parquet"
	}

	first := events[0]
	t := first.Time.UTC()

	// Exchange from envelope
	exchange := first.Exchange
	if exchange == "" {
		exchange = "unknown"
	}

	// Symbol from the exchange-specific payload
	symbol := ""
	switch exchange {
	case models.ExchangeBinance:
		if first.Binance != nil {
			symbol = first.Binance.Symbol
		}
	case models.ExchangeBybit:
		if first.Bybit != nil {
			symbol = first.Bybit.Symbol
		}
	case models.ExchangeOKX:
		if first.OKX != nil {
			symbol = first.OKX.Symbol
		}
	}
	if symbol == "" {
		symbol = "unknown"
	}

	// date=YYYY-MM-DD
	dateStr := t.Format("2006-01-02")

	// Filename WITHOUT UUID:
	//   binance_liq_ARBUSDT_20251116T073502.parquet
	ts := t.Format("20060102T150405")
	filename := fmt.Sprintf("%s_liq_%s_%s.parquet", exchange, symbol, ts)

	key := filepath.Join(
		fmt.Sprintf("exchange=%s", exchange),
		"market=liquidation",
		fmt.Sprintf("symbol=%s", symbol),
		fmt.Sprintf("date=%s", dateStr),
		filename,
	)

	// S3 wants forward slashes
	return filepath.ToSlash(key)
}

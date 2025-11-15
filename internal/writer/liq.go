package writer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
	pqwriter "github.com/xitongsys/parquet-go/writer"
	"log"
	"time"

	"cryptoflow/internal/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// group by exchange and write one Parquet object per exchange + time bucket
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
		if err := w.writeBatchSingleExchange(ctx, ex, group); err != nil {
			return err
		}
	}
	return nil
}

// unified Parquet row (superset of fields)
type parquetNormalizedLiquidation struct {
	Exchange     string `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Symbol       string `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Side         string `parquet:"name=side, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
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

func (w *S3ParquetWriter) writeBatchSingleExchange(
	ctx context.Context,
	exchange string,
	events []models.NormalizedLiquidation,
) error {
	if len(events) == 0 {
		return nil
	}

	t := events[0].Time.UTC().Truncate(time.Hour)
	key := fmt.Sprintf("%s%s/%04d/%02d/%02d/%02d/%s.parquet",
		w.cfg.Prefix,
		exchange,
		t.Year(), t.Month(), t.Day(),
		t.Hour(),
		uuid.New().String(),
	)

	var buf bytes.Buffer

	pw, err := pqwriter.NewParquetWriterFromWriter(&buf, new(parquetNormalizedLiquidation), 4)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128 MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, env := range events {
		row := parquetNormalizedLiquidation{
			Exchange:   env.Exchange,
			TimeMillis: env.Time.UTC().UnixNano() / int64(time.Millisecond),
		}

		switch env.Exchange {
		case models.ExchangeBinance:
			if env.Binance != nil {
				b := env.Binance
				row.Symbol = b.Symbol
				row.Side = b.Side
				row.PositionSide = b.PositionSide
				row.OrderType = b.OrderType
				row.Quantity = b.Quantity
				row.Price = b.Price
				row.AvgPrice = b.AvgPrice
				row.LastQty = b.LastQty
				row.LastPrice = b.LastPrice
				row.TradeID = b.TradeID
				row.IsMaker = b.IsMaker
				row.IsReduceOnly = b.IsReduceOnly
				row.WorkingType = b.WorkingType
				row.OriginalType = b.OriginalType
				row.CloseAll = b.CloseAll
				row.RealizedPnl = b.RealizedPnl
			}

		case models.ExchangeBybit:
			if env.Bybit != nil {
				b := env.Bybit
				row.Symbol = b.Symbol
				row.Side = b.Side
				row.PositionSide = "" // not present
				row.OrderType = "LIQUIDATION"
				row.Quantity = b.Quantity
				row.Price = b.Price
				// rest left zero
			}

		case models.ExchangeOKX:
			if env.OKX != nil {
				o := env.OKX
				row.Symbol = o.Symbol
				row.Side = o.Side
				row.PositionSide = o.PositionSide
				row.OrderType = "LIQUIDATION"
				row.Quantity = o.Quantity
				row.Price = o.Price
				// rest left zero
			}
		}

		if err := pw.Write(row); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("parquet write row: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("parquet write stop: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}

	if _, err := w.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("s3 put object: %w", err)
	}

	log.Printf("[s3-parquet-writer] wrote %d events -> s3://%s/%s", len(events), w.cfg.Bucket, key)
	return nil
}

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"
)

type Flattener struct {
	config     *appconfig.Config
	rawChan    <-chan models.RawFOBSMessage
	NormFOBSch chan<- models.BatchFOBSMessage
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log

	// Batching
	batches   map[string]*models.BatchFOBSMessage
	lastFlush map[string]time.Time

	// Metrics
	messagesProcessed int64
	batchesProcessed  int64
	errorsCount       int64
	entriesProcessed  int64
}

func NewFlattener(cfg *appconfig.Config, rawChan <-chan models.RawFOBSMessage, NormFOBSch chan<- models.BatchFOBSMessage) *Flattener {
	return &Flattener{
		config:     cfg,
		rawChan:    rawChan,
		NormFOBSch: NormFOBSch,
		wg:         &sync.WaitGroup{},
		log:        logger.GetLogger(),
		batches:    make(map[string]*models.BatchFOBSMessage),
		lastFlush:  make(map[string]time.Time),
	}
}

func (f *Flattener) Start(ctx context.Context) error {
	f.mu.Lock()
	if f.running {
		f.mu.Unlock()
		return fmt.Errorf("flattener already running")
	}
	f.running = true
	f.ctx = ctx
	f.mu.Unlock()

	log := f.log.WithComponent("flattener").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting flattener")

	// Start multiple workers for parallel processing
	numWorkers := f.config.Processor.MaxWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}

	log.WithFields(logger.Fields{"workers": numWorkers}).Info("starting flattener workers")

	for i := 0; i < numWorkers; i++ {
		f.wg.Add(1)
		go f.worker(i)
	}

	// Start batch flusher
	f.wg.Add(1)
	go f.batchFlusher()

	// Start metrics reporter
	go f.metricsReporter(ctx)

	log.Info("flattener started successfully")
	return nil
}

func (f *Flattener) Stop() {
	f.mu.Lock()
	f.running = false
	f.mu.Unlock()

	f.log.WithComponent("flattener").Info("stopping flattener")

	// Flush remaining batches
	f.flushAllBatches()

	f.wg.Wait()
	f.log.WithComponent("flattener").Info("flattener stopped")
}

func (f *Flattener) worker(workerID int) {
	defer f.wg.Done()

	log := f.log.WithComponent("flattener").WithFields(logger.Fields{
		"worker_id": workerID,
		"worker":    "flattener",
	})

	log.Info("starting flattener worker")

	for {
		select {
		case <-f.ctx.Done():
			log.Info("worker stopped due to context cancellation")
			return
		case rawMsg, ok := <-f.rawChan:
			if !ok {
				log.Info("raw channel closed, worker stopping")
				return
			}

			start := time.Now()
			entriesProcessed := f.processMessage(rawMsg)
			duration := time.Since(start)

			f.messagesProcessed++
			f.entriesProcessed += int64(entriesProcessed)

			logger.LogPerformanceEntry(log, "flattener", "process_message", duration, logger.Fields{
				"worker_id":         workerID,
				"exchange":          rawMsg.Exchange,
				"symbol":            rawMsg.Symbol,
				"entries_processed": entriesProcessed,
				"message_type":      rawMsg.MessageType,
			})
		}
	}
}

func (f *Flattener) processMessage(rawMsg models.RawFOBSMessage) int {
	log := f.log.WithComponent("flattener").WithFields(logger.Fields{
		"exchange":     rawMsg.Exchange,
		"symbol":       rawMsg.Symbol,
		"message_type": rawMsg.MessageType,
		"timestamp":    rawMsg.Timestamp,
		"operation":    "process_message",
	})

	log.Info("processing raw message")

	// Parse the raw orderbook data
	var binanceResp models.BinanceFOBSresp
	err := json.Unmarshal(rawMsg.Data, &binanceResp)
	if err != nil {
		f.errorsCount++
		log.WithError(err).Warn("failed to unmarshal orderbook data")
		return 0
	}

	// Flatten the orderbook data
	entries := f.flattenOrderbook(rawMsg, binanceResp)
	if len(entries) == 0 {
		log.Warn("no entries flattened from message")
		return 0
	}

	// Add to batch
	f.addToBatch(rawMsg, entries)

	log.WithFields(logger.Fields{
		"entries_count": len(entries),
		"bids_count":    len(binanceResp.Bids),
		"asks_count":    len(binanceResp.Asks),
	}).Info("message processed successfully")

	logger.LogDataFlowEntry(log, "raw_channel", "flattened_channel", len(entries), "flattened_entries")

	return len(entries)
}

func (f *Flattener) flattenOrderbook(rawMsg models.RawFOBSMessage, orderbook models.BinanceFOBSresp) []models.NormFOBSMessage {
	var entries []models.NormFOBSMessage
	normalSymbol := symbols.ToBinance(rawMsg.Exchange, rawMsg.Symbol)

	// Process bids (buy orders) - highest price first
	for level, bid := range orderbook.Bids {
		price, err := strconv.ParseFloat(bid[0], 64)
		if err != nil {
			f.errorsCount++
			f.log.WithComponent("flattener").WithError(err).WithFields(logger.Fields{
				"exchange":  rawMsg.Exchange,
				"symbol":    rawMsg.Symbol,
				"side":      "bid",
				"level":     level + 1,
				"raw_price": bid[0],
			}).Warn("failed to parse bid price")
			continue
		}

		quantity, err := strconv.ParseFloat(bid[1], 64)
		if err != nil {
			f.errorsCount++
			f.log.WithComponent("flattener").WithError(err).WithFields(logger.Fields{
				"exchange":     rawMsg.Exchange,
				"symbol":       rawMsg.Symbol,
				"side":         "bid",
				"level":        level + 1,
				"raw_quantity": bid[1],
			}).Warn("failed to parse bid quantity")
			continue
		}

		if price == 0 || quantity == 0 {
			continue
		}

		entries = append(entries, models.NormFOBSMessage{
			Exchange:     rawMsg.Exchange,
			Symbol:       normalSymbol,
			Market:       rawMsg.Market,
			Timestamp:    rawMsg.Timestamp,
			LastUpdateID: orderbook.LastUpdateID,
			Side:         "bid",
			Price:        price,
			Quantity:     quantity,
			Level:        level + 1,
		})
	}

	// Process asks (sell orders) - lowest price first
	for level, ask := range orderbook.Asks {
		price, err := strconv.ParseFloat(ask[0], 64)
		if err != nil {
			f.errorsCount++
			f.log.WithComponent("flattener").WithError(err).WithFields(logger.Fields{
				"exchange":  rawMsg.Exchange,
				"symbol":    rawMsg.Symbol,
				"side":      "ask",
				"level":     level + 1,
				"raw_price": ask[0],
			}).Warn("failed to parse ask price")
			continue
		}

		quantity, err := strconv.ParseFloat(ask[1], 64)
		if err != nil {
			f.errorsCount++
			f.log.WithComponent("flattener").WithError(err).WithFields(logger.Fields{
				"exchange":     rawMsg.Exchange,
				"symbol":       rawMsg.Symbol,
				"side":         "ask",
				"level":        level + 1,
				"raw_quantity": ask[1],
			}).Warn("failed to parse ask quantity")
			continue
		}

		if price == 0 || quantity == 0 {
			continue
		}

		entries = append(entries, models.NormFOBSMessage{
			Exchange:     rawMsg.Exchange,
			Symbol:       normalSymbol,
			Market:       rawMsg.Market,
			Timestamp:    rawMsg.Timestamp,
			LastUpdateID: orderbook.LastUpdateID,
			Side:         "ask",
			Price:        price,
			Quantity:     quantity,
			Level:        level + 1,
		})
	}

	return entries
}

func (f *Flattener) addToBatch(rawMsg models.RawFOBSMessage, entries []models.NormFOBSMessage) {
	normalSymbol := symbols.ToBinance(rawMsg.Exchange, rawMsg.Symbol)
	f.mu.Lock()
	defer f.mu.Unlock()

	batchKey := fmt.Sprintf("%s_%s_%s", rawMsg.Exchange, rawMsg.Market, normalSymbol)

	batch, exists := f.batches[batchKey]
	if !exists {
		batch = &models.BatchFOBSMessage{
			BatchID:     uuid.New().String(),
			Exchange:    rawMsg.Exchange,
			Symbol:      normalSymbol,
			Market:      rawMsg.Market,
			Entries:     make([]models.NormFOBSMessage, 0, f.config.Processor.BatchSize),
			RecordCount: 0,
			Timestamp:   rawMsg.Timestamp,
			ProcessedAt: time.Now(),
		}
		f.batches[batchKey] = batch
		f.lastFlush[batchKey] = time.Now()
	}

	// Add entries to batch
	batch.Entries = append(batch.Entries, entries...)
	batch.RecordCount = len(batch.Entries)

	// Update batch timestamp to latest message
	if rawMsg.Timestamp.After(batch.Timestamp) {
		batch.Timestamp = rawMsg.Timestamp
	}

	// Check if batch should be flushed
	if batch.RecordCount >= f.config.Processor.BatchSize {
		f.flushBatch(batchKey)
	}
}

func (f *Flattener) batchFlusher() {
	defer f.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.flushTimedOutBatches()
		}
	}
}

func (f *Flattener) flushTimedOutBatches() {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()
	for batchKey, lastFlush := range f.lastFlush {
		if now.Sub(lastFlush) >= f.config.Processor.BatchTimeout {
			f.flushBatch(batchKey)
		}
	}
}

func (f *Flattener) flushBatch(batchKey string) {
	batch, exists := f.batches[batchKey]
	if !exists || batch.RecordCount == 0 {
		return
	}

	log := f.log.WithComponent("flattener").WithFields(logger.Fields{
		"batch_id":     batch.BatchID,
		"batch_key":    batchKey,
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"record_count": batch.RecordCount,
		"operation":    "flush_batch",
	})

	log.Info("flushing batch")

	select {
	case f.NormFOBSch <- *batch:
		f.batchesProcessed++
		delete(f.batches, batchKey)
		delete(f.lastFlush, batchKey)

		log.Info("batch flushed successfully")
		logger.LogDataFlowEntry(log, "flattener", "flattened_channel", batch.RecordCount, "batch")

	case <-f.ctx.Done():
		return
	default:
		log.Warn("flattened channel is full, batch not sent")
	}
}

func (f *Flattener) flushAllBatches() {
	f.mu.Lock()
	defer f.mu.Unlock()

	log := f.log.WithComponent("flattener").WithFields(logger.Fields{"operation": "flush_all_batches"})
	log.Info("flushing all remaining batches")

	for batchKey := range f.batches {
		f.flushBatch(batchKey)
	}

	log.WithFields(logger.Fields{"remaining_batches": len(f.batches)}).Info("all batches flushed")
}

func (f *Flattener) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.reportMetrics()
		}
	}
}

func (f *Flattener) reportMetrics() {
	f.mu.RLock()
	messagesProcessed := f.messagesProcessed
	batchesProcessed := f.batchesProcessed
	errorsCount := f.errorsCount
	entriesProcessed := f.entriesProcessed
	activeBatches := len(f.batches)
	f.mu.RUnlock()

	errorRate := float64(0)
	if messagesProcessed+errorsCount > 0 {
		errorRate = float64(errorsCount) / float64(messagesProcessed+errorsCount)
	}

	avgEntriesPerMessage := float64(0)
	if messagesProcessed > 0 {
		avgEntriesPerMessage = float64(entriesProcessed) / float64(messagesProcessed)
	}
	rawLen := len(f.rawChan)
	rawCap := cap(f.rawChan)
	normLen := len(f.NormFOBSch)
	normCap := cap(f.NormFOBSch)

	log := f.log.WithComponent("flattener")
	f.log.LogMetric("flattener", "messages_processed", messagesProcessed, "counter", logger.Fields{})
	f.log.LogMetric("flattener", "batches_processed", batchesProcessed, "counter", logger.Fields{})
	f.log.LogMetric("flattener", "entries_processed", entriesProcessed, "counter", logger.Fields{})
	f.log.LogMetric("flattener", "errors_count", errorsCount, "counter", logger.Fields{})
	f.log.LogMetric("flattener", "error_rate", errorRate, "gauge", logger.Fields{})
	f.log.LogMetric("flattener", "active_batches", activeBatches, "gauge", logger.Fields{})
	f.log.LogMetric("flattener", "avg_entries_per_message", avgEntriesPerMessage, "gauge", logger.Fields{})

	log.WithFields(logger.Fields{
		"messages_processed":      messagesProcessed,
		"batches_processed":       batchesProcessed,
		"entries_processed":       entriesProcessed,
		"errors_count":            errorsCount,
		"error_rate":              errorRate,
		"active_batches":          activeBatches,
		"avg_entries_per_message": avgEntriesPerMessage,
		"raw_channel_len":         rawLen,
		"raw_channel_cap":         rawCap,
		"norm_channel_len":        normLen,
		"norm_channel_cap":        normCap,
	}).Info("flattener metrics")
}

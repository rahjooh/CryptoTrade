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
	fobs "cryptoflow/internal/channel/fobs"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"
)

type Flattener struct {
	config   *appconfig.Config
	channels *fobs.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log

	// Batching
	batches   map[string]*models.BatchFOBSMessage
	lastFlush map[string]time.Time
}

// channelBacklogDelay yields a small pause after each processed message so workers yield
// CPU time when the inbound queue briefly empties. Adjust if throughput profiling warrants.
const channelBacklogDelay = 5 * time.Millisecond

func NewFlattener(cfg *appconfig.Config, ch *fobs.Channels) *Flattener {
	return &Flattener{
		config:    cfg,
		channels:  ch,
		wg:        &sync.WaitGroup{},
		log:       logger.GetLogger(),
		batches:   make(map[string]*models.BatchFOBSMessage),
		lastFlush: make(map[string]time.Time),
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
		case rawMsg, ok := <-f.channels.Raw:
			if !ok {
				log.Info("raw channel closed, worker stopping")
				return
			}

			f.processMessage(rawMsg)
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

	log.Debug("processing raw message")

	// Parse the raw orderbook data
	var book models.BinanceFOBSresp
	switch rawMsg.Exchange {
	case "bybit":
		var bybitResp models.BybitFOBSresp
		if err := json.Unmarshal(rawMsg.Data, &bybitResp); err != nil {
			log.WithError(err).Warn("failed to unmarshal orderbook data")
			return 0
		}
		book = models.BinanceFOBSresp{
			LastUpdateID: bybitResp.UpdateID,
			Bids:         bybitResp.Bids,
			Asks:         bybitResp.Asks,
		}
	default:
		if err := json.Unmarshal(rawMsg.Data, &book); err != nil {
			log.WithError(err).Warn("failed to unmarshal orderbook data")
			return 0
		}
	}

	// Flatten the orderbook data
	entries := f.flattenOrderbook(rawMsg, book)
	if len(entries) == 0 {
		log.Warn("no entries flattened from message")
		return 0
	}

	// Add to batch
	f.addToBatch(rawMsg, entries)

	log.WithFields(logger.Fields{
		"entries_count": len(entries),
		"bids_count":    len(book.Bids),
		"asks_count":    len(book.Asks),
	}).Debug("message processed successfully")

	return len(entries)
}

func (f *Flattener) flattenOrderbook(rawMsg models.RawFOBSMessage, orderbook models.BinanceFOBSresp) []models.NormFOBSMessage {
	var entries []models.NormFOBSMessage
	normalSymbol := symbols.ToBinance(rawMsg.Exchange, rawMsg.Symbol)

	// Process bids (buy orders) - highest price first
	for level, bid := range orderbook.Bids {
		price, err := strconv.ParseFloat(bid[0], 64)
		if err != nil {
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

	log.Debug("flushing batch")

	if f.channels.SendNorm(f.ctx, *batch) {
		delete(f.batches, batchKey)
		delete(f.lastFlush, batchKey)

		log.Debug("batch flushed successfully")

	} else if f.ctx.Err() != nil {
		return
	} else {
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

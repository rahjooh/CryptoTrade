package processor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"
)

type Sorter struct {
	config        *config.Config
	flattenedChan <-chan models.FlattenedOrderbookBatch
	sortedChan    chan<- models.SortedOrderbookBatch
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Logger

	// Buffering
	buffer        map[string][]models.FlattenedOrderbookEntry // key: exchange-market-symbol
	lastFlushTime map[string]time.Time

	// Metrics
	batchesCreated  int64
	entriesBuffered int64
	bufferFlushes   int64
}

func NewSorter(cfg *config.Config, flattenedChan <-chan models.FlattenedOrderbookBatch, sortedChan chan<- models.SortedOrderbookBatch) *Sorter {
	log := logger.GetLogger()

	sorter := &Sorter{
		config:        cfg,
		flattenedChan: flattenedChan,
		sortedChan:    sortedChan,
		wg:            &sync.WaitGroup{},
		log:           log,
		buffer:        make(map[string][]models.FlattenedOrderbookEntry),
		lastFlushTime: make(map[string]time.Time),
	}

	log.WithComponent("sorter").Info("sorter initialized")
	return sorter
}

func (s *Sorter) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("sorter already running")
	}
	s.running = true
	s.ctx = ctx
	s.mu.Unlock()

	log := s.log.WithComponent("sorter").WithFields(logger.Fields{"operation": "start"})
	log.Info("starting sorter")

	// Start entry processing worker
	s.wg.Add(1)
	go s.entryProcessor()

	// Start buffer flusher
	s.wg.Add(1)
	go s.bufferFlusher()

	// Start metrics reporter
	go s.metricsReporter(ctx)

	log.Info("sorter started successfully")
	return nil
}

func (s *Sorter) Stop() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()

	s.log.WithComponent("sorter").Info("stopping sorter")

	// Flush remaining buffers before stopping
	s.flushAllBuffers()

	s.wg.Wait()
	s.log.WithComponent("sorter").Info("sorter stopped")
}

func (s *Sorter) entryProcessor() {
	defer s.wg.Done()

	log := s.log.WithComponent("sorter").WithFields(logger.Fields{"worker": "entry_processor"})
	log.Info("starting entry processor worker")

	for {
		select {
		case <-s.ctx.Done():
			log.Info("entry processor stopped due to context cancellation")
			return
		case batch, ok := <-s.flattenedChan:
			if !ok {
				log.Info("flattened channel closed, entry processor stopping")
				return
			}

			for _, entry := range batch.Entries {
				s.processEntry(entry)
			}
		}
	}
}

func (s *Sorter) processEntry(entry models.FlattenedOrderbookEntry) {
	key := fmt.Sprintf("%s-%s-%s", entry.Exchange, entry.Market, entry.Symbol)

	s.mu.Lock()

	// Initialize buffer if needed
	if _, exists := s.buffer[key]; !exists {
		s.buffer[key] = make([]models.FlattenedOrderbookEntry, 0, s.config.Writer.Batch.Size)
		s.lastFlushTime[key] = time.Now()
	}

	// Add entry to buffer
	s.buffer[key] = append(s.buffer[key], entry)
	s.entriesBuffered++

	bufferSize := len(s.buffer[key])
	s.mu.Unlock()

	log := s.log.WithComponent("sorter").WithFields(logger.Fields{
		"exchange":    entry.Exchange,
		"symbol":      entry.Symbol,
		"buffer_size": bufferSize,
		"batch_limit": s.config.Writer.Batch.Size,
	})
	log.Debug("entry added to buffer")

	// Check if buffer is ready for flushing
	if bufferSize >= s.config.Writer.Batch.Size {
		s.flushBuffer(key, "size_threshold")
	}
}

func (s *Sorter) bufferFlusher() {
	defer s.wg.Done()

	log := s.log.WithComponent("sorter").WithFields(logger.Fields{"worker": "buffer_flusher"})
	log.Info("starting buffer flusher worker")

	ticker := time.NewTicker(s.config.Writer.Buffer.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Info("buffer flusher stopped due to context cancellation")
			return
		case <-ticker.C:
			s.flushStaleBuffers()
		}
	}
}

func (s *Sorter) flushStaleBuffers() {
	s.mu.RLock()
	keysToFlush := make([]string, 0)
	now := time.Now()

	for key, lastFlush := range s.lastFlushTime {
		if now.Sub(lastFlush) >= s.config.Writer.Batch.Timeout && len(s.buffer[key]) > 0 {
			keysToFlush = append(keysToFlush, key)
		}
	}
	s.mu.RUnlock()

	for _, key := range keysToFlush {
		s.flushBuffer(key, "timeout")
	}

	if len(keysToFlush) > 0 {
		s.log.WithComponent("sorter").WithFields(logger.Fields{
			"flushed_buffers": len(keysToFlush),
			"reason":          "timeout",
		}).Debug("flushed stale buffers")
	}
}

func (s *Sorter) flushBuffer(key, reason string) {
	s.mu.Lock()
	entries, exists := s.buffer[key]
	if !exists || len(entries) == 0 {
		s.mu.Unlock()
		return
	}

	// Copy entries and reset buffer
	entriesToFlush := make([]models.FlattenedOrderbookEntry, len(entries))
	copy(entriesToFlush, entries)
	s.buffer[key] = s.buffer[key][:0] // Reset slice but keep capacity
	s.lastFlushTime[key] = time.Now()
	s.mu.Unlock()

	start := time.Now()

	// Sort entries
	s.sortEntries(entriesToFlush)

	// Create batch
	batch := s.createBatch(entriesToFlush)

	sortDuration := time.Since(start)

	log := s.log.WithComponent("sorter").WithFields(logger.Fields{
		"key":           key,
		"exchange":      batch.Exchange,
		"symbol":        batch.Symbol,
		"batch_id":      batch.BatchID,
		"record_count":  batch.RecordCount,
		"reason":        reason,
		"sort_duration": sortDuration.Milliseconds(),
	})
	log.Info("buffer flushed and batch created")

	logger.LogPerformanceEntry(log, "sorter", "sort_and_batch", sortDuration, logger.Fields{
		"exchange":     batch.Exchange,
		"symbol":       batch.Symbol,
		"record_count": batch.RecordCount,
		"reason":       reason,
	})

	// Send batch
	select {
	case s.sortedChan <- batch:
		s.batchesCreated++
		s.bufferFlushes++
		logger.LogDataFlowEntry(log, "flattened_channel", "sorted_channel", batch.RecordCount, "sorted_batch")
	case <-s.ctx.Done():
		return
	default:
		log.WithFields(logger.Fields{
			"batch_id":     batch.BatchID,
			"record_count": batch.RecordCount,
		}).Error("sorted channel full, dropping batch")
	}
}

func (s *Sorter) sortEntries(entries []models.FlattenedOrderbookEntry) {
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]

		// Primary sort: timestamp
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.Before(b.Timestamp)
		}

		// Secondary sort: exchange
		if a.Exchange != b.Exchange {
			return a.Exchange < b.Exchange
		}

		// Tertiary sort: symbol
		if a.Symbol != b.Symbol {
			return a.Symbol < b.Symbol
		}

		// Quaternary sort: side (bids first)
		if a.Side != b.Side {
			return a.Side == "bid"
		}

		// Final sort: level
		return a.Level < b.Level
	})
}

func (s *Sorter) createBatch(entries []models.FlattenedOrderbookEntry) models.SortedOrderbookBatch {
	if len(entries) == 0 {
		return models.SortedOrderbookBatch{}
	}

	batchID := fmt.Sprintf("%s-%s-%s-%d-%d",
		entries[0].Exchange,
		entries[0].Market,
		entries[0].Symbol,
		entries[0].Timestamp.Unix(),
		time.Now().UnixNano())

	return models.SortedOrderbookBatch{
		Exchange:    entries[0].Exchange,
		Symbol:      entries[0].Symbol,
		Market:      entries[0].Market,
		Timestamp:   entries[0].Timestamp,
		BatchID:     batchID,
		Entries:     entries,
		RecordCount: len(entries),
	}
}

func (s *Sorter) flushAllBuffers() {
	log := s.log.WithComponent("sorter").WithFields(logger.Fields{"operation": "flush_all_buffers"})

	s.mu.RLock()
	keys := make([]string, 0, len(s.buffer))
	for key := range s.buffer {
		if len(s.buffer[key]) > 0 {
			keys = append(keys, key)
		}
	}
	s.mu.RUnlock()

	log.WithFields(logger.Fields{"buffers_to_flush": len(keys)}).Info("flushing all remaining buffers")

	for _, key := range keys {
		s.flushBuffer(key, "shutdown")
	}
}

func (s *Sorter) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reportMetrics()
		}
	}
}

func (s *Sorter) reportMetrics() {
	s.mu.RLock()
	batchesCreated := s.batchesCreated
	entriesBuffered := s.entriesBuffered
	bufferFlushes := s.bufferFlushes

	// Calculate current buffer usage
	totalBufferedEntries := 0
	bufferCount := len(s.buffer)
	for _, entries := range s.buffer {
		totalBufferedEntries += len(entries)
	}
	s.mu.RUnlock()

	avgEntriesPerBatch := float64(0)
	if batchesCreated > 0 {
		avgEntriesPerBatch = float64(entriesBuffered) / float64(batchesCreated)
	}

	log := s.log.WithComponent("sorter")
	log.LogMetric("sorter", "batches_created", batchesCreated, logger.Fields{})
	log.LogMetric("sorter", "entries_buffered", entriesBuffered, logger.Fields{})
	log.LogMetric("sorter", "buffer_flushes", bufferFlushes, logger.Fields{})
	log.LogMetric("sorter", "current_buffer_count", bufferCount, logger.Fields{})
	log.LogMetric("sorter", "current_buffered_entries", totalBufferedEntries, logger.Fields{})
	log.LogMetric("sorter", "avg_entries_per_batch", avgEntriesPerBatch, logger.Fields{})

	log.WithFields(logger.Fields{
		"batches_created":          batchesCreated,
		"entries_buffered":         entriesBuffered,
		"buffer_flushes":           bufferFlushes,
		"current_buffer_count":     bufferCount,
		"current_buffered_entries": totalBufferedEntries,
		"avg_entries_per_batch":    avgEntriesPerBatch,
	}).Info("sorter metrics")
}

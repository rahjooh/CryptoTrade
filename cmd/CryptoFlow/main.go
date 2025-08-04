// cmd/CryptoFlow/main.go
package main

import (
	"CryptoFlow/internal/config"
	"CryptoFlow/internal/logger"
	"CryptoFlow/internal/metrics"
	"CryptoFlow/internal/model"
	"CryptoFlow/internal/reader"
	"CryptoFlow/internal/writer"
	"context"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	logger.Init()
	metrics.Init()
	config.EnsureTableDirs()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleShutdown(cancel)

	log.Info().Msg("CryptoFlow started.")

	wg := &sync.WaitGroup{}
	for _, symbol := range config.Symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			runSymbolWorker(ctx, sym)
		}(symbol)
	}

	wg.Wait()
}

func runSymbolWorker(ctx context.Context, symbol string) {
	client := config.NewHTTPClient()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	pool := writer.NewWriterPool()
	defer pool.CloseAll()

	for {
		select {
		case <-ticker.C:
			snapshot, err := reader.FetchOrderBookSnapshot(ctx, client, symbol)
			if err != nil {
				metrics.IncrementError(symbol)
				log.Error().Str("symbol", symbol).Err(err).Msg("Fetch failed")
				continue
			}

			ts := time.UnixMilli(snapshot.Timestamp)
			symLower := strings.ToLower(symbol)

			for _, b := range snapshot.Bids {
				row := &model.OrderBookSnapshotRow{Timestamp: snapshot.Timestamp, Price: b.Price, Quantity: b.Quantity}
				w, _ := pool.GetWriter(symLower, "bids", ts)
				_ = w.Write(row)
			}
			for _, a := range snapshot.Asks {
				row := &model.OrderBookSnapshotRow{Timestamp: snapshot.Timestamp, Price: a.Price, Quantity: a.Quantity}
				w, _ := pool.GetWriter(symLower, "asks", ts)
				_ = w.Write(row)
			}

			metrics.IncrementSuccess(symbol)
			log.Info().Str("symbol", symbol).
				Time("ts", ts).
				Int("bids", len(snapshot.Bids)).
				Int("asks", len(snapshot.Asks)).
				Msg("Snapshot written")

		case <-ctx.Done():
			log.Warn().Str("symbol", symbol).Msg("Shutdown requested.")
			return
		}
	}
}

func handleShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	cancel()
}

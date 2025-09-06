package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"cryptoflow/config"
	"cryptoflow/internal/channel"
	"cryptoflow/logger"
	"cryptoflow/processor"
	"cryptoflow/reader/binance"
	"cryptoflow/reader/kucoin"
	"cryptoflow/writer"
)

func main() {
	log := logger.GetLogger()

	// Load environment variables from .env if present
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Warn("Error loading .env file")
	}

	configPath := flag.String("config", "config/config.yml", "Path to configuration file")
	shardPath := flag.String("shards", "config/ip_shards.yml", "Path to IP shard configuration file")

	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.WithError(err).Error("Failed to load configuration")
		os.Exit(1)
	}

	if err := log.Configure(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output, cfg.Logging.MaxAge); err != nil {
		log.WithError(err).Error("Failed to configure logger")
		os.Exit(1)
	}

	log.WithFields(logger.Fields{
		"service": cfg.Cryptoflow.Name,
		"version": cfg.Cryptoflow.Version,
	}).Info("starting cryptoflow")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if strings.ToLower(cfg.Logging.Level) == "report" {
		logger.StartReport(ctx, log, 30*time.Second)
	}

	channels := channel.NewChannels(
		cfg.Channels.RawBuffer,
		cfg.Channels.ProcessedBuffer,
	)
	defer channels.Close()

	go channels.StartMetricsReporting(ctx)

	shardCfg, err := config.LoadIPShards(*shardPath)
	if err != nil {
		log.WithError(err).Error("failed to load shard configuration")
		os.Exit(1)
	}

	binanceFOBSReaders := make([]*binance.Binance_FOBS_Reader, 0, len(shardCfg.Shards))
	kucoinFOBSReaders := make([]*kucoin.Kucoin_FOBS_Reader, 0, len(shardCfg.Shards))
	binanceFOBDReaders := make([]*binance.Binance_FOBD_Reader, 0, len(shardCfg.Shards))
	kucoinFOBDReaders := make([]*kucoin.Kucoin_FOBD_Reader, 0, len(shardCfg.Shards))

	binanceSymbolSet := make(map[string]struct{})
	kucoinSymbolSet := make(map[string]struct{})

	for _, shard := range shardCfg.Shards {
		sc := *cfg
		sc.Source.Binance.Future.Orderbook.Snapshots.Symbols = shard.BinanceSymbols
		sc.Source.Binance.Future.Orderbook.Delta.Symbols = shard.BinanceSymbols
		sc.Source.Kucoin.Future.Orderbook.Snapshots.Symbols = shard.KucoinSymbols
		sc.Source.Kucoin.Future.Orderbook.Delta.Symbols = shard.KucoinSymbols

		binanceFOBSReaders = append(binanceFOBSReaders, binance.Binance_FOBS_NewReader(&sc, channels.FOBS.Raw, shard.BinanceSymbols, shard.IP))
		kucoinFOBSReaders = append(kucoinFOBSReaders, kucoin.Kucoin_FOBS_NewReader(&sc, channels.FOBS.Raw, shard.KucoinSymbols, shard.IP))
		binanceFOBDReaders = append(binanceFOBDReaders, binance.Binance_FOBD_NewReader(&sc, channels.FOBD.Raw, shard.BinanceSymbols, shard.IP))
		kucoinFOBDReaders = append(kucoinFOBDReaders, kucoin.Kucoin_FOBD_NewReader(&sc, channels.FOBD.Raw, shard.KucoinSymbols, shard.IP))

		for _, s := range shard.BinanceSymbols {
			binanceSymbolSet[s] = struct{}{}
		}
		for _, s := range shard.KucoinSymbols {
			kucoinSymbolSet[s] = struct{}{}
		}
	}

	// Aggregate symbols for processor filtering
	binanceAll := make([]string, 0, len(binanceSymbolSet))
	for s := range binanceSymbolSet {
		binanceAll = append(binanceAll, s)
	}
	kucoinAll := make([]string, 0, len(kucoinSymbolSet))
	for s := range kucoinSymbolSet {
		kucoinAll = append(kucoinAll, s)
	}

	cfg.Source.Binance.Future.Orderbook.Snapshots.Symbols = binanceAll
	cfg.Source.Binance.Future.Orderbook.Delta.Symbols = binanceAll
	cfg.Source.Kucoin.Future.Orderbook.Snapshots.Symbols = kucoinAll
	cfg.Source.Kucoin.Future.Orderbook.Delta.Symbols = kucoinAll

	norm_FOBS_reader := processor.NewFlattener(cfg, channels.FOBS.Raw, channels.FOBS.Norm)
	norm_FOBD_reader := processor.NewDeltaProcessor(cfg, channels.FOBD.Raw, channels.FOBD.Norm)

	var snapshotWriter *writer.SnapshotWriter
	var deltaWriter *writer.DeltaWriter

	if cfg.Storage.S3.Enabled {
		var err error
		snapshotWriter, err = writer.NewSnapshotWriter(cfg, channels.FOBS.Norm)
		if err != nil {
			log.WithError(err).Error("failed to create S3 writer")
			os.Exit(1)
		}
		deltaWriter, err = writer.NewDeltaWriter(cfg, channels.FOBD.Norm)
		if err != nil {
			log.WithError(err).Error("failed to create delta writer")
			os.Exit(1)
		}
	} else {
		log.WithComponent("main").Info("S3 storage disabled; skipping writers")
	}

	var wg sync.WaitGroup

	for _, r := range binanceFOBSReaders {
		wg.Add(1)
		go func(reader *binance.Binance_FOBS_Reader) {
			defer wg.Done()
			if err := reader.Binance_FOBS_Start(ctx); err != nil {
				log.WithError(err).Warn("binance reader failed to start")
			}
		}(r)
	}

	for _, r := range kucoinFOBSReaders {
		wg.Add(1)
		go func(reader *kucoin.Kucoin_FOBS_Reader) {
			defer wg.Done()
			if err := reader.Kucoin_FOBS_Start(ctx); err != nil {
				log.WithError(err).Warn("kucoin reader failed to start")
			}
		}(r)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := norm_FOBS_reader.Start(ctx); err != nil {
			log.WithError(err).Warn("norm_FOBS_reader failed to start")
		}
	}()

	for _, r := range binanceFOBDReaders {
		wg.Add(1)
		go func(reader *binance.Binance_FOBD_Reader) {
			defer wg.Done()
			if err := reader.Binance_FOBD_Start(ctx); err != nil {
				log.WithError(err).Warn("delta reader failed to start")
			}
		}(r)
	}

	for _, r := range kucoinFOBDReaders {
		wg.Add(1)
		go func(reader *kucoin.Kucoin_FOBD_Reader) {
			defer wg.Done()
			if err := reader.Kucoin_FOBD_Start(ctx); err != nil {
				log.WithError(err).Warn("kucoin delta reader failed to start")
			}
		}(r)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := norm_FOBD_reader.Start(ctx); err != nil {
			log.WithError(err).Warn("norm_FOBD_readerfailed to start")
		}
	}()

	if snapshotWriter != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := snapshotWriter.Start(ctx); err != nil {
				log.WithError(err).Warn("s3 writer failed to start")
			}
		}()
	}
	if deltaWriter != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := deltaWriter.Start(ctx); err != nil {
				log.WithError(err).Warn("delta writer failed to start")
			}
		}()
	}

	time.Sleep(2 * time.Second)
	log.Info("all components started successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.WithFields(logger.Fields{"signal": sig.String()}).Info("shutdown signal received")

	log.Info("starting graceful shutdown")
	cancel()

	if snapshotWriter != nil {
		log.Info("stopping S3 writer")
		snapshotWriter.Stop()
	}
	if deltaWriter != nil {
		log.Info("stopping delta writer")
		deltaWriter.Stop()
	}

	log.Info("stopping norm_FOBD_reader")
	norm_FOBD_reader.Stop()

	log.Info("stopping norm_FOBS_reader")
	norm_FOBS_reader.Stop()

	log.Info("stopping binance_FOBD_readers")
	for _, r := range binanceFOBDReaders {
		r.Binance_FOBD_Stop()
	}

	log.Info("stopping kucoin delta readers")
	for _, r := range kucoinFOBDReaders {
		r.Kucoin_FOBD_Stop()
	}

	log.Info("stopping binance readers")
	for _, r := range binanceFOBSReaders {
		r.Binance_FOBS_Stop()
	}

	log.Info("stopping kucoin readers")
	for _, r := range kucoinFOBSReaders {
		r.Kucoin_FOBS_Stop()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("graceful shutdown completed")
	case <-time.After(30 * time.Second):
		log.Warn("graceful shutdown timeout exceeded")
	}

	log.Info("cryptoflow stopped")
}

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

	configPath := flag.String("config", "config.yml", "Path to configuration file")
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

	binance_FOBS_reader := binance.Binance_FOBS_NewReader(cfg, channels.FOBS.Raw)
	kucoin_FOBS_reader := kucoin.Kucoin_FOBS_NewReader(cfg, channels.FOBS.Raw)
	norm_FOBS_reader := processor.NewFlattener(cfg, channels.FOBS.Raw, channels.FOBS.Norm)

	binance_FOBD_reader := binance.Binance_FOBD_NewReader(cfg, channels.FOBD.Raw)
	kucoin_FOBD_reader := kucoin.Kucoin_FOBD_NewReader(cfg, channels.FOBD.Raw)
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := binance_FOBS_reader.Binance_FOBS_Start(ctx); err != nil {
			log.WithError(err).Warn("binance reader failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := kucoin_FOBS_reader.Kucoin_FOBS_Start(ctx); err != nil {
			log.WithError(err).Warn("kucoin reader failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := norm_FOBS_reader.Start(ctx); err != nil {
			log.WithError(err).Warn("norm_FOBS_reader failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := binance_FOBD_reader.Binance_FOBD_Start(ctx); err != nil {
			log.WithError(err).Warn("delta reader failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := kucoin_FOBD_reader.Kucoin_FOBD_Start(ctx); err != nil {
			log.WithError(err).Warn("kucoin delta reader failed to start")
		}
	}()

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

	log.Info("stopping binance_FOBD_reader reader")
	binance_FOBD_reader.Binance_FOBD_Stop()

	log.Info("stopping kucoin delta reader")
	kucoin_FOBD_reader.Kucoin_FOBD_Stop()

	log.Info("stopping binance reader")
	binance_FOBS_reader.Binance_FOBS_Stop()

	log.Info("stopping kucoin reader")
	kucoin_FOBS_reader.Kucoin_FOBS_Stop()

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

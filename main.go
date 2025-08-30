package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"cryptoflow/config"
	"cryptoflow/internal/channel"
	"cryptoflow/logger"
	"cryptoflow/processor"
	"cryptoflow/reader/binance"
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

	channels := channel.NewChannels(
		cfg.Channels.RawBuffer,
		cfg.Channels.ProcessedBuffer,
	)
	defer channels.Close()

	go channels.StartMetricsReporting(ctx)

	binanceReader := binance.NewBinanceReader(cfg, channels.FOBS.Raw)
	flattener := processor.NewFlattener(cfg, channels.FOBS.Raw, channels.FOBS.Norm)

	deltaReader := binance.BinanceDeltaReader(cfg, channels.FOBD.Raw)
	deltaProcessor := processor.NewDeltaProcessor(cfg, channels.FOBD.Raw, channels.FOBD.Norm)

	var s3Writer *writer.S3Writer
	var deltaWriter *writer.DeltaWriter
	if cfg.Storage.S3.Enabled {
		var err error
		s3Writer, err = writer.NewS3Writer(cfg, channels.FOBS.Norm)
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
		if err := binanceReader.Start(ctx); err != nil {
			log.WithError(err).Warn("binance reader failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := flattener.Start(ctx); err != nil {
			log.WithError(err).Warn("flattener failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := deltaReader.Start(ctx); err != nil {
			log.WithError(err).Warn("delta reader failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := deltaProcessor.Start(ctx); err != nil {
			log.WithError(err).Warn("delta processor failed to start")
		}
	}()

	if s3Writer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s3Writer.Start(ctx); err != nil {
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

	if s3Writer != nil {
		log.Info("stopping S3 writer")
		s3Writer.Stop()
	}
	if deltaWriter != nil {
		log.Info("stopping delta writer")
		deltaWriter.Stop()
	}

	log.Info("stopping delta processor")
	deltaProcessor.Stop()

	log.Info("stopping flattener")
	flattener.Stop()

	log.Info("stopping delta reader")
	deltaReader.Stop()

	log.Info("stopping binance reader")
	binanceReader.Stop()

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

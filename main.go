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
	"cryptoflow/internal"
	"cryptoflow/logger"
	"cryptoflow/processor"
	"cryptoflow/reader"
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

	if err := log.Configure(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output); err != nil {
		log.WithError(err).Error("Failed to configure logger")
		os.Exit(1)
	}

	logConfiguration(log, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channels := internal.NewChannels(
		cfg.Channels.RawBuffer,
		cfg.Channels.ProcessedBuffer,
	)
	defer channels.Close()

	go channels.StartMetricsReporting(ctx)

	binanceReader := reader.NewBinanceReader(cfg, channels.RawMessageChan)
	flattener := processor.NewFlattener(cfg, channels.RawMessageChan, channels.FlattenedChan)

	var (
		s3Writer    *writer.S3Writer
		kafkaWriter *writer.KafkaWriter
	)
	if cfg.Storage.S3.Enabled {
		var err error
		s3Writer, err = writer.NewS3Writer(cfg, channels.FlattenedChan)
		if err != nil {
			log.WithError(err).Error("failed to create S3 writer")
			os.Exit(1)
		}
	} else if cfg.Storage.Kafka.Enabled {
		var err error
		kafkaWriter, err = writer.NewKafkaWriter(cfg, channels.FlattenedChan)
		if err != nil {
			log.WithError(err).Error("failed to create Kafka writer")
			os.Exit(1)
		}
	} else {
		log.WithComponent("main").Debug("no storage writer configured; skipping writer")
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

	if s3Writer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s3Writer.Start(ctx); err != nil {
				log.WithError(err).Warn("s3 writer failed to start")
			}
		}()
	} else if kafkaWriter != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := kafkaWriter.Start(ctx); err != nil {
				log.WithError(err).Warn("kafka writer failed to start")
			}
		}()
	}

	time.Sleep(2 * time.Second)
	log.Debug("all components started successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.WithFields(logger.Fields{"signal": sig.String()}).Debug("shutdown signal received")

	log.Debug("starting graceful shutdown")
	cancel()

	if s3Writer != nil {
		log.Debug("stopping S3 writer")
		s3Writer.Stop()
	} else if kafkaWriter != nil {
		log.Debug("stopping Kafka writer")
		kafkaWriter.Stop()
	}

	log.Debug("stopping flattener")
	flattener.Stop()

	log.Debug("stopping binance reader")
	binanceReader.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Debug("graceful shutdown completed")
	case <-time.After(30 * time.Second):
		log.Warn("graceful shutdown timeout exceeded")
	}

	log.Debug("cryptoflow stopped")
}

func logConfiguration(log *logger.Log, cfg *config.Config) {
	symbols := strings.Join(cfg.Source.Binance.Future.Orderbook.Snapshots.Symbols, ",")
	log.WithFields(logger.Fields{
		"service":        cfg.Cryptoflow.Name,
		"version":        cfg.Cryptoflow.Version,
		"s3_table_arn":   cfg.Storage.S3.TableARN,
		"s3_region":      cfg.Storage.S3.Region,
		"symbols":        symbols,
		"batch_size":     cfg.Writer.Batch.Size,
		"flush_interval": cfg.Storage.S3.FlushInterval,
	}).Info("configuration")
}

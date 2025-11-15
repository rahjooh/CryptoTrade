package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"cryptoflow/config"
	"cryptoflow/internal/channel"
	"cryptoflow/internal/dashboard"
	"cryptoflow/internal/metrics"
	"cryptoflow/internal/processor"
	"cryptoflow/internal/reader/binance"
	bybitreader "cryptoflow/internal/reader/bybit"
	"cryptoflow/internal/reader/kucoin"
	okxreader "cryptoflow/internal/reader/okx"
	"cryptoflow/internal/writer"
	"cryptoflow/logger"
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

	metrics.Configure(cfg.Metrics)
	metrics.InitCloudWatch(cfg.Storage.S3.Region, cfg.Cryptoflow.Name, cfg.Logging.DashboardName)

	log.WithFields(logger.Fields{
		"service": cfg.Cryptoflow.Name,
		"version": cfg.Cryptoflow.Version,
	}).Info("starting cryptoflow")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dashboardServer, err := dashboard.NewServer(cfg.Dashboard, log)
	if err != nil {
		log.WithError(err).Error("failed to initialise dashboard server")
		os.Exit(1)
	}

	if dashboardServer != nil {
		go func() {
			if err := dashboardServer.Run(ctx, cfg.Cryptoflow.Name); err != nil {
				log.WithError(err).Error("dashboard server exited")
			}
		}()
		log.WithComponent("dashboard").WithFields(logger.Fields{"address": dashboardServer.Address()}).Info("dashboard available")
	}

	channels := channel.NewChannels(
		cfg.Channels.RawBuffer,
		cfg.Channels.ProcessedBuffer,
	)
	defer channels.Close()
	metrics.StartChannelSizeMetrics(ctx, channels, time.Second)

	shardCfg, err := config.LoadIPShards(*shardPath)
	if err != nil {
		log.WithError(err).Error("failed to load shard configuration")
		os.Exit(1)
	}

	env := config.AppEnvironment()
	detectedIPs, err := localIPv4Addresses()
	if err != nil {
		log.WithError(err).Warn("failed to enumerate local IP addresses")
	}

	ipSet := make([]string, 0, len(detectedIPs)+1)
	seenIPs := make(map[string]struct{}, len(detectedIPs)+1)
	for _, ip := range detectedIPs {
		if ip == "" {
			continue
		}
		if _, exists := seenIPs[ip]; exists {
			continue
		}
		seenIPs[ip] = struct{}{}
		ipSet = append(ipSet, ip)
	}
	if override := strings.TrimSpace(os.Getenv("SHARD_IP")); override != "" {
		if _, exists := seenIPs[override]; !exists {
			seenIPs[override] = struct{}{}
			ipSet = append(ipSet, override)
		}
	}
	sort.Strings(ipSet)

	activeShards := shardCfg.Shards
	if filtered := shardCfg.FilterByIP(ipSet); len(filtered) > 0 {
		activeShards = filtered
		log.WithFields(logger.Fields{
			"ips":            strings.Join(ipSet, ","),
			"matched_shards": len(activeShards),
		}).Info("using IP shard configuration for local host")
	} else if config.IsProductionLike(env) {
		log.WithFields(logger.Fields{
			"ips":         strings.Join(ipSet, ","),
			"environment": env,
		}).Error("no shard configuration matched local IPs")
		os.Exit(1)
	} else {
		log.WithFields(logger.Fields{
			"ips":         strings.Join(ipSet, ","),
			"environment": env,
		}).Warn("no shard configuration matched local IPs; defaulting to all shards")
	}

	binanceFOBSReaders := make([]*binance.Binance_FOBS_Reader, 0, len(activeShards))
	bybitFOBSReaders := make([]*bybitreader.Bybit_FOBS_Reader, 0, len(activeShards))
	kucoinFOBSReaders := make([]*kucoin.Kucoin_FOBS_Reader, 0, len(activeShards))
	okxFOBSReaders := make([]*okxreader.Okx_FOBS_Reader, 0, len(activeShards))
	binanceFOBDReaders := make([]*binance.Binance_FOBD_Reader, 0, len(activeShards))
	bybitFOBDReaders := make([]*bybitreader.Bybit_FOBD_Reader, 0, len(activeShards))
	kucoinFOBDReaders := make([]*kucoin.Kucoin_FOBD_Reader, 0, len(activeShards))
	okxFOBDReaders := make([]*okxreader.Okx_FOBD_Reader, 0, len(activeShards))
	binanceLiqReaders := make([]*binance.Binance_LIQ_Reader, 0, len(activeShards))
	bybitLiqReaders := make([]*bybitreader.Bybit_LIQ_Reader, 0, len(activeShards))
	kucoinLiqReaders := make([]*kucoin.Kucoin_LIQ_Reader, 0, len(activeShards))
	okxLiqReaders := make([]*okxreader.Okx_LIQ_Reader, 0, len(activeShards))
	binanceFOIReaders := make([]*binance.Binance_FOI_Reader, 0, len(activeShards))
	bybitFOIReaders := make([]*bybitreader.Bybit_FOI_Reader, 0, len(activeShards))
	kucoinFOIReaders := make([]*kucoin.Kucoin_FOI_Reader, 0, len(activeShards))
	okxFOIReaders := make([]*okxreader.Okx_FOI_Reader, 0, len(activeShards))
	binancePIReaders := make([]*binance.Binance_PI_Reader, 0, len(activeShards))
	bybitPIReaders := make([]*bybitreader.Bybit_PI_Reader, 0, len(activeShards))
	kucoinPIReaders := make([]*kucoin.Kucoin_PI_Reader, 0, len(activeShards))
	okxPIReaders := make([]*okxreader.Okx_PI_Reader, 0, len(activeShards))

	binanceSymbolSet := make(map[string]struct{})
	bybitSymbolSet := make(map[string]struct{})
	kucoinSymbolSet := make(map[string]struct{})
	okxSymbolSet := make(map[string]struct{})
	okxLiquidationSet := make(map[string]struct{})

	for _, shard := range activeShards {
		sc := *cfg
		sc.Source.Binance.Future.Orderbook.Snapshots.Symbols = shard.BinanceSymbols
		sc.Source.Binance.Future.Orderbook.Delta.Symbols = shard.BinanceSymbols
		sc.Source.Binance.Future.OpenInterest.Symbols = shard.BinanceSymbols
		sc.Source.Binance.Future.PremiumIndex.Symbols = shard.BinanceSymbols
		sc.Source.Bybit.Future.Orderbook.Snapshots.Symbols = shard.BybitSymbols
		sc.Source.Bybit.Future.Orderbook.Delta.Symbols = shard.BybitSymbols
		sc.Source.Bybit.Future.OpenInterest.Symbols = shard.BybitSymbols
		sc.Source.Bybit.Future.PremiumIndex.Symbols = shard.BybitSymbols
		sc.Source.Kucoin.Future.Orderbook.Snapshots.Symbols = shard.KucoinSymbols
		sc.Source.Kucoin.Future.Orderbook.Delta.Symbols = shard.KucoinSymbols
		sc.Source.Kucoin.Future.OpenInterest.Symbols = shard.KucoinSymbols
		sc.Source.Kucoin.Future.PremiumIndex.Symbols = shard.KucoinSymbols
		sc.Source.Okx.Future.Orderbook.Snapshots.Symbols = shard.OkxSymbols.SwapOrderbookSnapshot
		sc.Source.Okx.Future.Orderbook.Delta.Symbols = shard.OkxSymbols.SwapOrderbookDelta
		sc.Source.Okx.Future.OpenInterest.Symbols = shard.OkxSymbols.SwapOrderbookSnapshot
		sc.Source.Okx.Future.PremiumIndex.Symbols = shard.OkxSymbols.SwapOrderbookSnapshot
		sc.Source.Binance.Future.Liquidation.Symbols = shard.BinanceSymbols
		sc.Source.Bybit.Future.Liquidation.Symbols = shard.BybitSymbols
		sc.Source.Kucoin.Future.Liquidation.Symbols = shard.KucoinSymbols
		sc.Source.Okx.Future.Liquidation.Symbols = shard.OkxSymbols.SwapOrderbookSnapshot

		binanceFOBSReaders = append(binanceFOBSReaders, binance.Binance_FOBS_NewReader(&sc, channels.FOBS, shard.BinanceSymbols, shard.IP))
		bybitFOBSReaders = append(bybitFOBSReaders, bybitreader.Bybit_FOBS_NewReader(&sc, channels.FOBS, shard.BybitSymbols, shard.IP))
		kucoinFOBSReaders = append(kucoinFOBSReaders, kucoin.Kucoin_FOBS_NewReader(&sc, channels.FOBS, shard.KucoinSymbols, shard.IP))
		okxFOBSReaders = append(okxFOBSReaders, okxreader.Okx_FOBS_NewReader(&sc, channels.FOBS, shard.OkxSymbols.SwapOrderbookSnapshot, shard.IP))
		binanceFOBDReaders = append(binanceFOBDReaders, binance.Binance_FOBD_NewReader(&sc, channels.FOBD, shard.BinanceSymbols, shard.IP))
		bybitFOBDReaders = append(bybitFOBDReaders, bybitreader.Bybit_FOBD_NewReader(&sc, channels.FOBD, shard.BybitSymbols, shard.IP))
		kucoinFOBDReaders = append(kucoinFOBDReaders, kucoin.Kucoin_FOBD_NewReader(&sc, channels.FOBD, shard.KucoinSymbols, shard.IP))
		okxFOBDReaders = append(okxFOBDReaders, okxreader.Okx_FOBD_NewReader(&sc, channels.FOBD, shard.OkxSymbols.SwapOrderbookDelta, shard.IP))
		binanceLiqReaders = append(binanceLiqReaders, binance.Binance_LIQ_NewReader(&sc, channels.Liq, shard.BinanceSymbols))
		bybitLiqReaders = append(bybitLiqReaders, bybitreader.Bybit_LIQ_NewReader(&sc, channels.Liq, shard.BybitSymbols))
		kucoinLiqReaders = append(kucoinLiqReaders, kucoin.Kucoin_LIQ_NewReader(&sc, channels.Liq, shard.KucoinSymbols))
		okxLiqReaders = append(okxLiqReaders, okxreader.Okx_LIQ_NewReader(&sc, channels.Liq, shard.OkxSymbols.SwapOrderbookSnapshot, shard.IP))
		binanceFOIReaders = append(binanceFOIReaders, binance.Binance_FOI_NewReader(&sc, channels.FOI, shard.BinanceSymbols, shard.IP))
		bybitFOIReaders = append(bybitFOIReaders, bybitreader.Bybit_FOI_NewReader(&sc, channels.FOI, shard.BybitSymbols, shard.IP))
		kucoinFOIReaders = append(kucoinFOIReaders, kucoin.Kucoin_FOI_NewReader(&sc, channels.FOI, shard.KucoinSymbols))
		okxFOIReaders = append(okxFOIReaders, okxreader.Okx_FOI_NewReader(&sc, channels.FOI, shard.OkxSymbols.SwapOrderbookSnapshot, shard.IP))
		binancePIReaders = append(binancePIReaders, binance.Binance_PI_NewReader(&sc, channels.PI, shard.BinanceSymbols, shard.IP))
		bybitPIReaders = append(bybitPIReaders, bybitreader.Bybit_PI_NewReader(&sc, channels.PI, shard.BybitSymbols))
		kucoinPIReaders = append(kucoinPIReaders, kucoin.Kucoin_PI_NewReader(&sc, channels.PI, shard.KucoinSymbols))
		okxPIReaders = append(okxPIReaders, okxreader.Okx_PI_NewReader(&sc, channels.PI, shard.OkxSymbols.SwapOrderbookSnapshot, shard.IP))

		for _, s := range shard.BinanceSymbols {
			binanceSymbolSet[s] = struct{}{}
		}
		for _, s := range shard.BybitSymbols {
			bybitSymbolSet[s] = struct{}{}
		}
		for _, s := range shard.KucoinSymbols {
			kucoinSymbolSet[s] = struct{}{}
		}
		for _, s := range shard.OkxSymbols.SwapOrderbookSnapshot {
			okxSymbolSet[s] = struct{}{}
			okxLiquidationSet[s] = struct{}{}
		}
		for _, s := range shard.OkxSymbols.SwapOrderbookDelta {
			okxSymbolSet[s] = struct{}{}
		}
	}

	// Aggregate symbols for processor filtering
	binanceAll := make([]string, 0, len(binanceSymbolSet))
	for s := range binanceSymbolSet {
		binanceAll = append(binanceAll, s)
	}
	bybitAll := make([]string, 0, len(bybitSymbolSet))
	for s := range bybitSymbolSet {
		bybitAll = append(bybitAll, s)
	}
	kucoinAll := make([]string, 0, len(kucoinSymbolSet))
	for s := range kucoinSymbolSet {
		kucoinAll = append(kucoinAll, s)
	}
	okxAll := make([]string, 0, len(okxSymbolSet))
	for s := range okxSymbolSet {
		okxAll = append(okxAll, s)
	}
	okxLiqAll := make([]string, 0, len(okxLiquidationSet))
	for s := range okxLiquidationSet {
		okxLiqAll = append(okxLiqAll, s)
	}

	cfg.Source.Binance.Future.Orderbook.Snapshots.Symbols = binanceAll
	cfg.Source.Binance.Future.Orderbook.Delta.Symbols = binanceAll
	cfg.Source.Binance.Future.Liquidation.Symbols = binanceAll
	cfg.Source.Binance.Future.OpenInterest.Symbols = binanceAll
	cfg.Source.Binance.Future.PremiumIndex.Symbols = binanceAll

	cfg.Source.Bybit.Future.Orderbook.Snapshots.Symbols = bybitAll
	cfg.Source.Bybit.Future.Orderbook.Delta.Symbols = bybitAll
	cfg.Source.Bybit.Future.Liquidation.Symbols = bybitAll
	cfg.Source.Bybit.Future.OpenInterest.Symbols = bybitAll
	cfg.Source.Bybit.Future.PremiumIndex.Symbols = bybitAll

	cfg.Source.Kucoin.Future.Orderbook.Snapshots.Symbols = kucoinAll
	cfg.Source.Kucoin.Future.Orderbook.Delta.Symbols = kucoinAll
	cfg.Source.Kucoin.Future.Liquidation.Symbols = kucoinAll
	cfg.Source.Kucoin.Future.OpenInterest.Symbols = kucoinAll
	cfg.Source.Kucoin.Future.PremiumIndex.Symbols = kucoinAll

	cfg.Source.Okx.Future.Orderbook.Snapshots.Symbols = okxAll
	cfg.Source.Okx.Future.Orderbook.Delta.Symbols = okxAll
	cfg.Source.Okx.Future.Liquidation.Symbols = okxLiqAll
	cfg.Source.Okx.Future.OpenInterest.Symbols = okxAll
	cfg.Source.Okx.Future.PremiumIndex.Symbols = okxAll

	norm_FOBS_reader := processor.NewFlattener(cfg, channels.FOBS)
	norm_FOBD_reader := processor.NewDeltaProcessor(cfg, channels.FOBD)
	foiProcessor := processor.NewFOIProcessor(cfg, channels.FOI)
	piProcessor := processor.NewPIProcessor(cfg, channels.PI)
	liqProcessor := processor.NewLiquidationProcessor(cfg, channels.Liq)

	var snapshotWriter *writer.SnapshotWriter
	var deltaWriter *writer.DeltaWriter
	var liqWriter *writer.LiquidationWriter
	var foiWriter *writer.FOIWriter
	var piWriter *writer.PIWriter

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
		foiWriter, err = writer.NewFOIWriter(cfg, channels.FOI.Norm)
		if err != nil {
			log.WithError(err).Error("failed to create FOI writer")
			os.Exit(1)
		}
		if err := foiWriter.Start(ctx); err != nil {
			log.WithError(err).Error("failed to start FOI writer")
			os.Exit(1)
		}
		liqWriter, err = writer.NewLiquidationWriter(cfg, channels.Liq.Norm)
		if err != nil {
			log.WithError(err).Error("failed to create liquidation writer")
			os.Exit(1)
		}
		if err := liqWriter.Start(ctx); err != nil {
			log.WithError(err).Error("failed to start liquidation writer")
			os.Exit(1)
		}
		piWriter, err = writer.NewPIWriter(cfg, channels.PI.Norm)
		if err != nil {
			log.WithError(err).Error("failed to create premium-index writer")
			os.Exit(1)
		}
		if err := piWriter.Start(ctx); err != nil {
			log.WithError(err).Error("failed to start premium-index writer")
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

	for _, r := range bybitFOBSReaders {
		wg.Add(1)
		go func(reader *bybitreader.Bybit_FOBS_Reader) {
			defer wg.Done()
			if err := reader.Bybit_FOBS_Start(ctx); err != nil {
				log.WithError(err).Warn("bybit reader failed to start")
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

	for _, r := range okxFOBSReaders {
		wg.Add(1)
		go func(reader *okxreader.Okx_FOBS_Reader) {
			defer wg.Done()
			if err := reader.Okx_FOBS_Start(ctx); err != nil {
				log.WithError(err).Warn("okx reader failed to start")
			}
		}(r)
	}

	for _, r := range binanceLiqReaders {
		wg.Add(1)
		go func(reader *binance.Binance_LIQ_Reader) {
			defer wg.Done()
			if err := reader.Binance_LIQ_Start(ctx); err != nil {
				log.WithError(err).Warn("binance liquidation reader failed to start")
			}
		}(r)
	}

	for _, r := range bybitLiqReaders {
		wg.Add(1)
		go func(reader *bybitreader.Bybit_LIQ_Reader) {
			defer wg.Done()
			if err := reader.Bybit_LIQ_Start(ctx); err != nil {
				log.WithError(err).Warn("bybit liquidation reader failed to start")
			}
		}(r)
	}

	for _, r := range kucoinLiqReaders {
		wg.Add(1)
		go func(reader *kucoin.Kucoin_LIQ_Reader) {
			defer wg.Done()
			if err := reader.Kucoin_LIQ_Start(ctx); err != nil {
				log.WithError(err).Warn("kucoin liquidation reader failed to start")
			}
		}(r)
	}

	for _, r := range okxLiqReaders {
		wg.Add(1)
		go func(reader *okxreader.Okx_LIQ_Reader) {
			defer wg.Done()
			if err := reader.Okx_LIQ_Start(ctx); err != nil {
				log.WithError(err).Warn("okx liquidation reader failed to start")
			}
		}(r)
	}

	for _, r := range binanceFOIReaders {
		wg.Add(1)
		go func(reader *binance.Binance_FOI_Reader) {
			defer wg.Done()
			if err := reader.Binance_FOI_Start(ctx); err != nil {
				log.WithError(err).Warn("binance FOI reader failed to start")
			}
		}(r)
	}

	for _, r := range bybitFOIReaders {
		wg.Add(1)
		go func(reader *bybitreader.Bybit_FOI_Reader) {
			defer wg.Done()
			if err := reader.Bybit_FOI_Start(ctx); err != nil {
				log.WithError(err).Warn("bybit FOI reader failed to start")
			}
		}(r)
	}

	for _, r := range kucoinFOIReaders {
		wg.Add(1)
		go func(reader *kucoin.Kucoin_FOI_Reader) {
			defer wg.Done()
			if err := reader.Kucoin_FOI_Start(ctx); err != nil {
				log.WithError(err).Warn("kucoin FOI reader failed to start")
			}
		}(r)
	}

	for _, r := range okxFOIReaders {
		wg.Add(1)
		go func(reader *okxreader.Okx_FOI_Reader) {
			defer wg.Done()
			if err := reader.Okx_FOI_Start(ctx); err != nil {
				log.WithError(err).Warn("okx FOI reader failed to start")
			}
		}(r)
	}

	for _, r := range binancePIReaders {
		wg.Add(1)
		go func(reader *binance.Binance_PI_Reader) {
			defer wg.Done()
			if err := reader.Binance_PI_Start(ctx); err != nil {
				log.WithError(err).Warn("binance premium-index reader failed to start")
			}
		}(r)
	}

	for _, r := range bybitPIReaders {
		wg.Add(1)
		go func(reader *bybitreader.Bybit_PI_Reader) {
			defer wg.Done()
			if err := reader.Bybit_PI_Start(ctx); err != nil {
				log.WithError(err).Warn("bybit premium-index reader failed to start")
			}
		}(r)
	}

	for _, r := range kucoinPIReaders {
		wg.Add(1)
		go func(reader *kucoin.Kucoin_PI_Reader) {
			defer wg.Done()
			if err := reader.Kucoin_PI_Start(ctx); err != nil {
				log.WithError(err).Warn("kucoin premium-index reader failed to start")
			}
		}(r)
	}

	for _, r := range okxPIReaders {
		wg.Add(1)
		go func(reader *okxreader.Okx_PI_Reader) {
			defer wg.Done()
			if err := reader.Okx_PI_Start(ctx); err != nil {
				log.WithError(err).Warn("okx premium-index reader failed to start")
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := foiProcessor.Start(ctx); err != nil {
			log.WithError(err).Warn("foi processor failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := piProcessor.Start(ctx); err != nil {
			log.WithError(err).Warn("pi processor failed to start")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := liqProcessor.Start(ctx); err != nil {
			log.WithError(err).Warn("liquidation processor failed to start")
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

	for _, r := range bybitFOBDReaders {
		wg.Add(1)
		go func(reader *bybitreader.Bybit_FOBD_Reader) {
			defer wg.Done()
			if err := reader.Bybit_FOBD_Start(ctx); err != nil {
				log.WithError(err).Warn("bybit delta reader failed to start")
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

	for _, r := range okxFOBDReaders {
		wg.Add(1)
		go func(reader *okxreader.Okx_FOBD_Reader) {
			defer wg.Done()
			if err := reader.Okx_FOBD_Start(ctx); err != nil {
				log.WithError(err).Warn("okx delta reader failed to start")
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
	if liqWriter != nil {
		log.Info("stopping liquidation writer")
		liqWriter.Stop()
	}
	if foiWriter != nil {
		log.Info("stopping open-interest writer")
		foiWriter.Stop()
	}
	if piWriter != nil {
		log.Info("stopping premium-index writer")
		piWriter.Stop()
	}

	log.Info("stopping norm_FOBD_reader")
	norm_FOBD_reader.Stop()

	log.Info("stopping norm_FOBS_reader")
	norm_FOBS_reader.Stop()

	log.Info("stopping foi processor")
	foiProcessor.Stop()

	log.Info("stopping pi processor")
	piProcessor.Stop()

	log.Info("stopping liquidation processor")
	liqProcessor.Stop()

	log.Info("stopping binance_FOBD_readers")
	for _, r := range binanceFOBDReaders {
		r.Binance_FOBD_Stop()
	}

	log.Info("stopping bybit delta readers")
	for _, r := range bybitFOBDReaders {
		r.Bybit_FOBD_Stop()
	}

	log.Info("stopping kucoin delta readers")
	for _, r := range kucoinFOBDReaders {
		r.Kucoin_FOBD_Stop()
	}

	log.Info("stopping okx delta readers")
	for _, r := range okxFOBDReaders {
		r.Okx_FOBD_Stop()
	}

	log.Info("stopping binance readers")
	for _, r := range binanceFOBSReaders {
		r.Binance_FOBS_Stop()
	}

	log.Info("stopping bybit readers")
	for _, r := range bybitFOBSReaders {
		r.Bybit_FOBS_Stop()
	}

	log.Info("stopping kucoin readers")
	for _, r := range kucoinFOBSReaders {
		r.Kucoin_FOBS_Stop()
	}

	log.Info("stopping okx readers")
	for _, r := range okxFOBSReaders {
		r.Okx_FOBS_Stop()
	}

	log.Info("stopping binance liquidation readers")
	for _, r := range binanceLiqReaders {
		r.Binance_LIQ_Stop()
	}

	log.Info("stopping bybit liquidation readers")
	for _, r := range bybitLiqReaders {
		r.Bybit_LIQ_Stop()
	}

	log.Info("stopping kucoin liquidation readers")
	for _, r := range kucoinLiqReaders {
		r.Kucoin_LIQ_Stop()
	}

	log.Info("stopping okx liquidation readers")
	for _, r := range okxLiqReaders {
		r.Okx_LIQ_Stop()
	}

	log.Info("stopping binance open-interest readers")
	for _, r := range binanceFOIReaders {
		r.Binance_FOI_Stop()
	}

	log.Info("stopping bybit open-interest readers")
	for _, r := range bybitFOIReaders {
		r.Bybit_FOI_Stop()
	}

	log.Info("stopping kucoin open-interest readers")
	for _, r := range kucoinFOIReaders {
		r.Kucoin_FOI_Stop()
	}

	log.Info("stopping okx open-interest readers")
	for _, r := range okxFOIReaders {
		r.Okx_FOI_Stop()
	}

	log.Info("stopping binance premium-index readers")
	for _, r := range binancePIReaders {
		r.Binance_PI_Stop()
	}

	log.Info("stopping bybit premium-index readers")
	for _, r := range bybitPIReaders {
		r.Bybit_PI_Stop()
	}

	log.Info("stopping kucoin premium-index readers")
	for _, r := range kucoinPIReaders {
		r.Kucoin_PI_Stop()
	}

	log.Info("stopping okx premium-index readers")
	for _, r := range okxPIReaders {
		r.Okx_PI_Stop()
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

func localIPv4Addresses() ([]string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	ips := make([]string, 0, len(interfaces))
	for _, iface := range interfaces {
		if (iface.Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			if ipv4 := ip.To4(); ipv4 != nil {
				ips = append(ips, ipv4.String())
			}
		}
	}
	return ips, nil
}

package dispacherpackage

import "context"
dispatcher

import (
	"context"
	"CryptoFlow/internal/config"
	"CryptoFlow/internal/logger"
	"CryptoFlow/internal/pipeline"
	"CryptoFlow/internal/pipeline/fetcher"
)

/*
Wireframe:
----------
- For each exchange, enable relevant market categories (spot/future).
- For each enabled data type (snapshots, deltas, funding), launch the proper fetcher.
*/

func StartExchangeReaders(ctx context.Context, exchangeName string, exchange config.Exchange, ch *pipeline.PipelineChannels) {
	logger.Log.Infof("Starting readers for exchange: %s", exchangeName)
	if exchange.Spot != nil {
		logger.Log.Infof("→ Spot market enabled for %s", exchangeName)
		startMarketCategoryReaders(ctx, exchangeName, "spot", exchange.Spot, ch)
	}
	if exchange.Future != nil {
		logger.Log.Infof("→ Future market enabled for %s", exchangeName)
		startMarketCategoryReaders(ctx, exchangeName, "future", exchange.Future, ch)
	}
}

func startMarketCategoryReaders(ctx context.Context, exchange, marketType string, category *config.MarketCategory, ch *pipeline.PipelineChannels) {
	if category.Orderbook != nil && category.Orderbook.Snapshots != nil && category.Orderbook.Snapshots.Enabled {
		logger.Log.Infof("Launching snapshot REST pipeline for %s/%s", exchange, marketType)
		fetcher.StartSnapshotRESTReaders(ctx, exchange, marketType, "orderbook_snapshot", category.Orderbook.Snapshots, ch.LowFreqCh)
	}
	if category.Orderbook != nil && category.Orderbook.Delta != nil && category.Orderbook.Delta.Enabled {
		logger.Log.Infof("Launching delta WebSocket pipeline for %s/%s", exchange, marketType)
		fetcher.StartDeltaWSReaders(ctx, exchange, marketType, "orderbook_delta", category.Orderbook.Delta, ch.HighFreqCh)
	}
	if category.FundingRate != nil && category.FundingRate.Enabled {
		logger.Log.Infof("Launching funding rate REST pipeline for %s/%s", exchange, marketType)
		fetcher.StartSnapshotRESTReaders(ctx, exchange, marketType, "funding_rate", category.FundingRate, ch.MetadataCh)
	}
}


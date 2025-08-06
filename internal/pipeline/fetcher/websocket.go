package fetcher

//
//import (
//	"context"
//	"fmt"
//	"net/url"
//	"strings"
//	"time"
//
//	"github.com/gorilla/websocket"
//
//	"CryptoFlow/internal/logger"
//	"CryptoFlow/internal/model"
//	"CryptoFlow/internal/pipeline/normalizer"
//)
//
///*
//Wireframe:
//----------
//- For each enabled symbol, spin up a WebSocket connection to the exchange stream.
//- Read messages in a loop.
//- Normalize the data using `NormalizeDelta`.
//- Filter out messages with symbols not in the allowed list.
//- Push valid normalized data into the appropriate channel.
//*/
//
//// StartDeltaWSReaders initializes one WebSocket pipeline per symbol.
//// It builds an allowedSymbols map for filtering downstream.
//func StartDeltaWSReaders(ctx context.Context, exchange, marketType, dataType string, md *model.OrderBookDelta, out chan<- model.OrderBookDelta) {
//	allowedSymbols := make(map[string]bool)
//	for _, s := range md.Symbols {
//		allowedSymbols[s] = true
//	}
//
//	for _, symbol := range md.Symbols {
//		stream := fmt.Sprintf("%s@depth@100ms", symbol) // Adjust as needed for each exchange format
//		go startWSWorker(ctx, exchange, marketType, dataType, symbol, md.URL, stream, allowedSymbols, out)
//	}
//}
//
//// startWSWorker handles the lifecycle of a single WebSocket connection.
//func startWSWorker(
//	ctx context.Context,
//	exchange, marketType, dataType, symbol, baseURL, stream string,
//	allowedSymbols map[string]bool,
//	out chan<- config.MarketData,
//) {
//	// Build full WebSocket URL
//	wsURL := fmt.Sprintf("%s/%s", baseURL, stream)
//	u, err := url.Parse(wsURL)
//	if err != nil {
//		logger.Log.Error().
//			Str("exchange", exchange).Str("market_type", marketType).
//			Str("symbol", symbol).Str("url", wsURL).
//			Msgf("Invalid WebSocket URL: %v", err)
//		return
//	}
//
//	// Establish WebSocket connection
//	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
//	if err != nil {
//		logger.Log.Error().
//			Str("exchange", exchange).Str("market_type", marketType).
//			Str("symbol", symbol).Str("url", wsURL).
//			Msgf("WebSocket dial failed: %v", err)
//		return
//	}
//	defer conn.Close()
//
//	logger.Log.Info().
//		Str("exchange", exchange).Str("market_type", marketType).
//		Str("symbol", symbol).Str("stream", stream).
//		Msg("WebSocket connection established")
//
//	// Main read loop
//	for {
//		select {
//		case <-ctx.Done():
//			logger.Log.Info().
//				Str("exchange", exchange).Str("market_type", marketType).
//				Str("symbol", symbol).
//				Msg("WebSocket worker shutting down")
//			return
//
//		default:
//			_, message, err := conn.ReadMessage()
//			if err != nil {
//				logger.Log.Warn().
//					Str("exchange", exchange).Str("market_type", marketType).
//					Str("symbol", symbol).
//					Msgf("WebSocket read error: %v", err)
//				time.Sleep(2 * time.Second)
//				continue
//			}
//
//			// Normalize and filter
//			normalized, err := normalizer.NormalizeDelta(
//				exchange, marketType, dataType,
//				time.Now().UnixMilli(), message,
//				allowedSymbols,
//			)
//
//			// Filtered out symbol — do not log noisy errors
//			if err != nil {
//				if !strings.Contains(err.Error(), "not in allowed") {
//					logger.Log.Warn().
//						Str("exchange", exchange).Str("market_type", marketType).
//						Str("symbol", symbol).
//						Msgf("Normalization failed: %v", err)
//				}
//				continue
//			}
//
//			// Push to channel, drop if full
//			select {
//			case out <- normalized:
//			default:
//				logger.Log.Warn().
//					Str("exchange", exchange).Str("market_type", marketType).
//					Str("symbol", symbol).
//					Msg("WebSocket output channel full — dropping message")
//			}
//		}
//	}
//}

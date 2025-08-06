package fetcher

//
//import (
//	"context"
//	"io"
//	"net/http"
//
//	"CryptoFlow/internal/logger"
//)
//
//// FetchRESTSpotOrderBookSnapshot sends a REST request and returns the raw body bytes.
//func FetchRESTSpotOrderBookSnapshot(ctx context.Context, client *http.Client, url, exchange, marketType, symbol string) ([]byte, error) {
//	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
//	if err != nil {
//		logger.Log.Warn().
//			Str("exchange", exchange).
//			Str("market_type", marketType).
//			Str("symbol", symbol).
//			Msgf("REST request build failed: %v", err)
//		return nil, err
//	}
//
//	resp, err := client.Do(req)
//	if err != nil {
//		logger.Log.Warn().
//			Str("exchange", exchange).
//			Str("market_type", marketType).
//			Str("symbol", symbol).
//			Str("url", url).
//			Msgf("REST request failed: %v", err)
//		return nil, err
//	}
//	defer resp.Body.Close()
//
//	body, err := io.ReadAll(resp.Body)
//	if err != nil {
//		logger.Log.Warn().
//			Str("exchange", exchange).
//			Str("market_type", marketType).
//			Str("symbol", symbol).
//			Msgf("REST response read failed: %v", err)
//		return nil, err
//	}
//
//	return body, nil
//}

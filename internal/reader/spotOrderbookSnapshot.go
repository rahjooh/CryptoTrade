// Calls GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000
// Parses JSON snapshot response
package reader

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"CryptoFlow/internal/model"
)

func FetchOrderBookSnapshot(ctx context.Context, client *http.Client, symbol string) (*model.SpotOrderBookSnapshot, error) {
	url := "https://api.binance.com/api/v3/depth?symbol=" + symbol + "&limit=1000"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		LastUpdateID int         `json:"lastUpdateId"`
		Bids         [][2]string `json:"bids"`
		Asks         [][2]string `json:"asks"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	bids := make([]model.SpotOrderBookSnapshotPriceLevel, 0, len(result.Bids))
	asks := make([]model.SpotOrderBookSnapshotPriceLevel, 0, len(result.Asks))

	for _, b := range result.Bids {
		p, _ := strconv.ParseFloat(b[0], 64)
		q, _ := strconv.ParseFloat(b[1], 64)
		bids = append(bids, model.SpotOrderBookSnapshotPriceLevel{Price: p, Quantity: q})
	}
	for _, a := range result.Asks {
		p, _ := strconv.ParseFloat(a[0], 64)
		q, _ := strconv.ParseFloat(a[1], 64)
		asks = append(asks, model.SpotOrderBookSnapshotPriceLevel{Price: p, Quantity: q})
	}

	return &model.SpotOrderBookSnapshot{
		Timestamp: time.Now().UnixMilli(),
		Bids:      bids,
		Asks:      asks,
	}, nil
}

func StartSpotOrderBookReader(ctx context.Context, symbols []string, outChan chan<- *model.SpotOrderBookSnapshot) {
	for _, symbol := range symbols {
		go func(sym string) {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					snapshot, err := fetchBinanceSpotSnapshot(sym)
					if err != nil {
						log.Printf("Failed to fetch snapshot for %s: %v", sym, err)
						continue
					}
					outChan <- snapshot
				}
			}
		}(symbol)
	}
}

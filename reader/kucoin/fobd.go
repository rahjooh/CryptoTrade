package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"

	sdkapi "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/api"
	futurespublic "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/generate/futures/futurespublic"
	sdktype "github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/types"
)

// Kucoin_FOBD_Reader streams futures order book deltas from KuCoin.
type Kucoin_FOBD_Reader struct {
	config  *appconfig.Config
	rawChan chan<- models.RawFOBDMessage
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool
	log     *logger.Log
	symbols []string
	localIP string
}

// Kucoin_FOBD_NewReader creates a new delta reader.
// Symbols defines the markets this reader will subscribe to.
func Kucoin_FOBD_NewReader(cfg *appconfig.Config, rawChan chan<- models.RawFOBDMessage, symbols []string, localIP string) *Kucoin_FOBD_Reader {
	return &Kucoin_FOBD_Reader{
		config:  cfg,
		rawChan: rawChan,
		wg:      &sync.WaitGroup{},
		log:     logger.GetLogger(),
		symbols: symbols,
		localIP: localIP,
	}
}

// Kucoin_FOBD_Start subscribes to level2 streams for configured symbols.
func (r *Kucoin_FOBD_Reader) Kucoin_FOBD_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("delta reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.Orderbook.Delta
	log := r.log.WithComponent("kucoin_delta_reader").WithFields(logger.Fields{"operation": "Kucoin_FOBD_Start"})

	if !cfg.Enabled {
		log.Warn("kucoin futures orderbook delta is disabled")
		return fmt.Errorf("kucoin futures orderbook delta is disabled")
	}

	symbols := r.symbols
	if len(symbols) == 0 {
		symbols = cfg.Symbols
	}
	if len(symbols) == 0 {
		log.Warn("no symbols configured for kucoin futures orderbook delta")
		return fmt.Errorf("no symbols configured for kucoin futures orderbook delta")
	}

	log.WithFields(logger.Fields{"symbols": symbols}).Info("starting delta reader")

	r.wg.Add(1)
	go r.Kucoin_FOBD_stream(symbols, cfg.URL)

	log.Info("kucoin delta reader started successfully")
	return nil
}

// Kucoin_FOBD_Stop terminates all websocket subscriptions.
func (r *Kucoin_FOBD_Reader) Kucoin_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_delta_reader").Info("stopping delta reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_delta_reader").Info("delta reader stopped")
}

func parseChange(change string) (side, price, quantity string) {
	parts := strings.Split(change, ",")
	if len(parts) < 3 {
		return
	}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		switch p {
		case "buy", "sell":
			side = p
		default:
			if price == "" {
				price = p
			} else if quantity == "" {
				quantity = p
			}
		}
	}
	return
}

func (r *Kucoin_FOBD_Reader) Kucoin_FOBD_stream(symbolList []string, wsURL string) {
	defer r.wg.Done()

	baseURL := wsURL
	if parsed, err := url.Parse(wsURL); err == nil {
		baseURL = fmt.Sprintf("https://%s", parsed.Host)
	}

	wsOpt := sdktype.NewWebSocketClientOptionBuilder().Build()
	option := sdktype.NewClientOptionBuilder().
		WithFuturesEndpoint(baseURL).
		WithWebSocketClientOption(wsOpt).
		Build()

	client := sdkapi.NewClient(option)
	ws := client.WsService().NewFuturesPublicWS()

	log := r.log.WithComponent("kucoin_delta_reader").WithFields(logger.Fields{
		"worker": "delta_stream",
	})

	if err := ws.Start(); err != nil {
		log.WithError(err).Warn("failed to start websocket")
		return
	}
	defer ws.Stop()

	for _, symbol := range symbolList {
		_, err := ws.OrderbookIncrement(symbol, func(topic, subject string, data *futurespublic.OrderbookIncrementEvent) error {
			symbol := strings.TrimPrefix(topic, "/contractMarket/level2:")

			evt := models.KucoinFOBDResp{
				Symbol:    symbol,
				Sequence:  data.Sequence,
				Timestamp: data.Timestamp,
			}

			side, price, quantity := parseChange(data.Change)
			entry := models.FOBDEntry{Price: price, Quantity: quantity}
			switch side {
			case "buy":
				evt.Bids = []models.FOBDEntry{entry}
			case "sell":
				evt.Asks = []models.FOBDEntry{entry}
			}

			payload, err := json.Marshal(evt)
			if err != nil {
				log.WithError(err).Warn("failed to marshal event")
				return nil
			}

			msgOut := models.RawFOBDMessage{
				Exchange:  "kucoin",
				Symbol:    symbols.ToBinance("kucoin", symbol),
				Market:    "future-orderbook-delta",
				Data:      payload,
				Timestamp: time.Now(),
			}

			select {
			case r.rawChan <- msgOut:
				logger.IncrementDeltaRead(len(payload))
			case <-r.ctx.Done():
				return fmt.Errorf("context cancelled")
			default:
				log.Warn("raw delta channel full, dropping message")
			}
			return nil
		})
		if err != nil {
			log.WithFields(logger.Fields{"symbol": symbol}).WithError(err).Warn("failed to subscribe")
		}
	}

	<-r.ctx.Done()
}

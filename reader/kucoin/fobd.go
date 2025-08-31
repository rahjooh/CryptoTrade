package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"

	kumex "github.com/Kucoin/kucoin-futures-go-sdk"
)

// Delta streams futures order book deltas from KuCoin.
type Delta struct {
	config  *appconfig.Config
	rawChan chan<- models.RawFOBDMessage
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool
	log     *logger.Log
}

// KucoinDeltaReader creates a new delta reader.
func KucoinDeltaReader(cfg *appconfig.Config, rawChan chan<- models.RawFOBDMessage) *Delta {
	return &Delta{
		config:  cfg,
		rawChan: rawChan,
		wg:      &sync.WaitGroup{},
		log:     logger.GetLogger(),
	}
}

// Start subscribes to level2 streams for configured symbols.
func (r *Delta) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("delta reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.Orderbook.Delta
	log := r.log.WithComponent("kucoin_delta_reader").WithFields(logger.Fields{"operation": "start"})

	if !cfg.Enabled {
		log.Warn("kucoin futures orderbook delta is disabled")
		return fmt.Errorf("kucoin futures orderbook delta is disabled")
	}

	log.WithFields(logger.Fields{"symbols": cfg.Symbols}).Info("starting delta reader")

	for _, symbol := range cfg.Symbols {
		r.wg.Add(1)
		go r.streamSymbol(symbol)
	}

	log.Info("kucoin delta reader started successfully")
	return nil
}

// Stop terminates all websocket subscriptions.
func (r *Delta) Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_delta_reader").Info("stopping delta reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_delta_reader").Info("delta reader stopped")
}

func (r *Delta) streamSymbol(symbol string) {
	defer r.wg.Done()

	log := r.log.WithComponent("kucoin_delta_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "delta_stream",
	})

	service := kumex.NewApiService()
	rsp, err := service.WebSocketPublicToken()
	if err != nil {
		log.WithError(err).Warn("failed to get websocket token")
		return
	}

	tk := &kumex.WebSocketTokenModel{}
	if err := rsp.ReadData(tk); err != nil {
		log.WithError(err).Warn("failed to read websocket token")
		return
	}

	c := service.NewWebSocketClient(tk)
	mc, ec, err := c.Connect()
	if err != nil {
		log.WithError(err).Warn("failed to connect websocket")
		return
	}

	topic := fmt.Sprintf("/contractMarket/level2:%s", symbol)
	sub := kumex.NewSubscribeMessage(topic, false)
	if err := c.Subscribe(sub); err != nil {
		log.WithError(err).Warn("failed to subscribe")
		return
	}

	for {
		select {
		case <-r.ctx.Done():
			c.Stop()
			return
		case err := <-ec:
			if err != nil {
				log.WithError(err).Warn("websocket error")
			}
		case msg := <-mc:
			if msg.Topic != topic {
				continue
			}
			var data struct {
				Sequence  int64  `json:"sequence"`
				Symbol    string `json:"symbol"`
				Timestamp int64  `json:"timestamp"`
				Changes   struct {
					Bids [][]string `json:"bids"`
					Asks [][]string `json:"asks"`
				} `json:"changes"`
			}
			if err := msg.ReadData(&data); err != nil {
				log.WithError(err).Warn("failed to read level2 data")
				continue
			}

			evt := models.BinanceFOBDResp{
				Symbol:           symbols.ToBinance("kucoin", data.Symbol),
				Time:             data.Timestamp,
				FirstUpdateID:    data.Sequence,
				LastUpdateID:     data.Sequence,
				PrevLastUpdateID: data.Sequence - 1,
			}
			evt.Bids = make([]models.FOBDEntry, len(data.Changes.Bids))
			for i, b := range data.Changes.Bids {
				if len(b) < 2 {
					continue
				}
				evt.Bids[i] = models.FOBDEntry{Price: b[0], Quantity: b[1]}
			}
			evt.Asks = make([]models.FOBDEntry, len(data.Changes.Asks))
			for i, a := range data.Changes.Asks {
				if len(a) < 2 {
					continue
				}
				evt.Asks[i] = models.FOBDEntry{Price: a[0], Quantity: a[1]}
			}

			payload, err := json.Marshal(evt)
			if err != nil {
				log.WithError(err).Warn("failed to marshal event")
				continue
			}

			msgOut := models.RawFOBDMessage{
				Exchange:  "kucoin",
				Symbol:    symbols.ToBinance("kucoin", data.Symbol),
				Market:    "future-orderbook-delta",
				Data:      payload,
				Timestamp: time.Now(),
			}

			select {
			case r.rawChan <- msgOut:
				logger.IncrementDeltaRead(len(payload))
			case <-r.ctx.Done():
				c.Stop()
				return
			default:
				log.Warn("raw delta channel full, dropping message")
			}
		}
	}
}

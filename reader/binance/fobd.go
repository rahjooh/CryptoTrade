package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"

	futures "github.com/adshao/go-binance/v2/futures"
	"github.com/sirupsen/logrus"
)

// BinanceDeltaReader streams futures order book deltas from Binance.
// It uses the websocket diff depth stream with a configurable interval
// and forwards raw messages to the provided channel.
type Delta struct {
	config  *appconfig.Config
	rawChan chan<- models.RawFOBDmodel
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool
	log     *logger.Log
}

// BinanceDeltaReader creates a new delta reader using binance-go client.
func BinanceDeltaReader(cfg *appconfig.Config, rawChan chan<- models.RawFOBDmodel) *Delta {
	return &Delta{
		config:  cfg,
		rawChan: rawChan,
		wg:      &sync.WaitGroup{},
		log:     logger.GetLogger(),
	}
}

// Start subscribes to diff depth streams for configured symbols.
func (r *Delta) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("delta reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.Orderbook.Delta
	log := r.log.WithComponent("binance_delta_reader").WithFields(logger.Fields{"operation": "start"})

	if !cfg.Enabled {
		log.Warn("binance futures orderbook delta is disabled")
		return fmt.Errorf("binance futures orderbook delta is disabled")
	}

	log.WithFields(logger.Fields{"symbols": cfg.Symbols, "interval": cfg.IntervalMs}).Info("starting delta reader")

	for _, symbol := range cfg.Symbols {
		r.wg.Add(1)
		go r.streamSymbol(symbol, time.Duration(cfg.IntervalMs)*time.Millisecond)
	}

	log.Info("binance delta reader started successfully")
	return nil
}

// Stop terminates all websocket subscriptions.
func (r *Delta) Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_delta_reader").Info("stopping delta reader")
	r.wg.Wait()
	r.log.WithComponent("binance_delta_reader").Info("delta reader stopped")
}

func (r *Delta) streamSymbol(symbol string, interval time.Duration) {
	defer r.wg.Done()

	log := r.log.WithComponent("binance_delta_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "delta_stream",
	})

	handler := func(event *futures.WsDepthEvent) {
		payload, err := json.Marshal(event)
		if err != nil {
			log.WithError(err).Warn("failed to marshal depth event")
			return
		}

		msg := models.RawFOBDmodel{
			Exchange:  "binance",
			Symbol:    event.Symbol,
			Market:    "future-orderbook-delta",
			Data:      payload,
			Timestamp: time.Now(),
		}

		select {
		case r.rawChan <- msg:
			if log.Logger.IsLevelEnabled(logrus.DebugLevel) {
				logger.LogDataFlowEntry(log, "binance_ws", "rawfobd", len(event.Bids)+len(event.Asks), "delta_entries")
			}
		case <-r.ctx.Done():
		default:
			log.Warn("raw delta channel full, dropping message")
		}
	}

	errHandler := func(err error) {
		if err != nil {
			log.WithError(err).Warn("websocket error")
		}
	}

	doneC, stopC, err := futures.WsDiffDepthServeWithRate(symbol, interval, handler, errHandler)
	if err != nil {
		log.WithError(err).Error("failed to subscribe to diff depth stream")
		return
	}

	select {
	case <-r.ctx.Done():
		close(stopC)
		<-doneC
	case <-doneC:
		// stream ended
	}
}

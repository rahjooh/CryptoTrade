package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobd "cryptoflow/internal/channel/fobd"
	ratemetrics "cryptoflow/internal/metrics/rate"
	"cryptoflow/logger"
	"cryptoflow/models"

	futures "github.com/adshao/go-binance/v2/futures"
	"github.com/sirupsen/logrus"
)

// Binance_FOBD_Reader streams futures order book deltas from Binance.
// It uses the websocket diff depth stream with a configurable interval
// and forwards raw messages to the provided channel.
type Binance_FOBD_Reader struct {
	config   *appconfig.Config
	channels *fobd.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	wsWeight *ratemetrics.WSWeightTracker
	ip       string
}

// Binance_FOBD_NewReader creates a new delta reader using binance-go client.
// Symbols defines the set of markets this reader will subscribe to. The localIP
// parameter is reserved for future use when websocket dialers support binding to
// specific interfaces.
func Binance_FOBD_NewReader(cfg *appconfig.Config, ch *fobd.Channels, symbols []string, localIP string) *Binance_FOBD_Reader {
	return &Binance_FOBD_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		wsWeight: ratemetrics.NewWSWeightTracker(),
		ip:       localIP,
	}
}

// Binance_FOBD_Start subscribes to diff depth streams for configured symbols.
func (r *Binance_FOBD_Reader) Binance_FOBD_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("Binance_FOBD_Reader reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.Orderbook.Delta
	log := r.log.WithComponent("binance_delta_reader").WithFields(logger.Fields{"operation": "Binance_FOBD_Start"})

	if !cfg.Enabled {
		log.Warn("binance futures orderbook delta is disabled")
		return fmt.Errorf("binance futures orderbook delta is disabled")
	}

	log.WithFields(logger.Fields{"symbols": r.symbols, "interval": cfg.IntervalMs}).Info("starting delta reader")

	r.wg.Add(1)
	go r.Binance_FOBD_stream(r.symbols)

	log.Info("binance delta reader started successfully")
	return nil
}

// Binance_FOBD_Stop terminates all websocket subscriptions.
func (r *Binance_FOBD_Reader) Binance_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_delta_reader").Info("stopping delta reader")
	r.wg.Wait()
	r.log.WithComponent("binance_delta_reader").Info("delta reader stopped")
}

func (r *Binance_FOBD_Reader) Binance_FOBD_stream(symbols []string) {
	defer r.wg.Done()

	log := r.log.WithComponent("binance_delta_reader").WithFields(logger.Fields{
		"symbols": strings.Join(symbols, ","),
		"worker":  "delta_stream",
	})

	handler := func(event *futures.WsDepthEvent) {
		payload, err := json.Marshal(event)
		if err != nil {
			log.WithError(err).Warn("failed to marshal depth event")
			return
		}

		msg := models.RawFOBDMessage{
			Exchange:  "binance",
			Symbol:    event.Symbol,
			Market:    "future-orderbook-delta",
			Data:      payload,
			Timestamp: time.Now(),
		}

		if r.channels.SendRaw(r.ctx, msg) {
			if log.Logger.IsLevelEnabled(logrus.DebugLevel) {
				logger.LogDataFlowEntry(log, "binance_ws", "rawfobd", len(event.Bids)+len(event.Asks), "delta_entries")
			}
			logger.IncrementDeltaRead(len(payload))
		} else if r.ctx.Err() != nil {
			return
		} else {
			log.Warn("raw delta channel full, dropping message")
		}
	}

	errHandler := func(err error) {
		if err != nil {
			log.WithError(err).Warn("websocket error")
		}
	}

	for {
		r.wsWeight.RegisterConnectionAttempt()
		r.wsWeight.RegisterOutgoing(len(symbols))
		ratemetrics.ReportWSWeight(r.log, r.wsWeight, r.ip)
		doneC, stopC, err := futures.WsCombinedDiffDepthServe(symbols, handler, errHandler)
		if err != nil {
			log.WithError(err).Error("failed to subscribe to combined diff depth stream")
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		select {
		case <-r.ctx.Done():
			close(stopC)
			<-doneC
			return
		case <-doneC:
			log.Warn("delta stream closed, reconnecting")
			close(stopC)
			select {
			case <-r.ctx.Done():
				<-doneC
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

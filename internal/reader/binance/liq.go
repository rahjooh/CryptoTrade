package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	liq "cryptoflow/internal/channel/liq"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	futures "github.com/adshao/go-binance/v2/futures"
	"github.com/sirupsen/logrus"
)

// Binance_LIQ_Reader streams liquidation orders from the Binance futures
// websocket API and forwards raw payloads to the configured channel.
type Binance_LIQ_Reader struct {
	config   *appconfig.Config
	channels *liq.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
}

// Binance_LIQ_NewReader constructs a new liquidation reader.
func Binance_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels, symbols []string) *Binance_LIQ_Reader {
	return &Binance_LIQ_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Binance_LIQ_Start launches websocket subscriptions for each configured
// symbol. Subscriptions are restarted automatically until the context is
// cancelled.
func (r *Binance_LIQ_Reader) Binance_LIQ_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("binance liquidation reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.Liquidation
	log := r.log.WithComponent("binance_liq_reader").WithFields(logger.Fields{"operation": "Binance_LIQ_Start"})

	if !cfg.Enabled {
		log.Warn("binance futures liquidation stream disabled via configuration")
		return fmt.Errorf("binance futures liquidation stream disabled")
	}
	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			log.Warn("no symbols configured for binance liquidation reader")
			return fmt.Errorf("no symbols configured for binance liquidation reader")
		}
		r.symbols = cfg.Symbols
	}

	log.WithFields(logger.Fields{"symbols": strings.Join(r.symbols, ",")}).Info("starting binance liquidation reader")

	for _, symbol := range r.symbols {
		sym := strings.ToUpper(symbol)
		r.wg.Add(1)
		go r.streamSymbol(sym)
	}

	log.Info("binance liquidation reader started successfully")
	return nil
}

// Binance_LIQ_Stop waits for all symbol workers to stop.
func (r *Binance_LIQ_Reader) Binance_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_liq_reader").Info("stopping binance liquidation reader")
	r.wg.Wait()
	r.log.WithComponent("binance_liq_reader").Info("binance liquidation reader stopped")
}

func (r *Binance_LIQ_Reader) streamSymbol(symbol string) {
	defer r.wg.Done()

	log := r.log.WithComponent("binance_liq_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "liquidation_stream",
	})

	handler := func(event *futures.WsLiquidationOrderEvent) {
		payload, err := json.Marshal(event)
		if err != nil {
			log.WithError(err).Warn("failed to marshal liquidation event")
			return
		}

		ts := time.UnixMilli(event.Time).UTC()
		msg := models.RawLiquidationMessage{
			Exchange:  "binance",
			Symbol:    strings.ToUpper(event.LiquidationOrder.Symbol),
			Market:    "liquidation",
			Data:      payload,
			Timestamp: ts,
		}

		if r.channels.SendRaw(r.ctx, msg) {
			if log.Logger.IsLevelEnabled(logrus.DebugLevel) {
				log.WithFields(logger.Fields{
					"payload_bytes": len(payload),
					"side":          event.LiquidationOrder.Side,
				}).Debug("forwarded liquidation event to raw channel")
			}
		} else if r.ctx.Err() != nil {
			return
		} else {
			log.Warn("liquidation raw channel full, dropping message")
		}
	}

	errHandler := func(err error) {
		if err != nil {
			log.WithError(err).Warn("websocket error")
		}
	}

	for {
		if r.ctx.Err() != nil {
			return
		}

		doneC, stopC, err := futures.WsLiquidationOrderServe(symbol, handler, errHandler)
		if err != nil {
			log.WithError(err).Error("failed to subscribe to liquidation stream")
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		select {
		case <-r.ctx.Done():
			close(stopC)
			<-doneC
			return
		case <-doneC:
			log.Warn("liquidation stream closed, reconnecting")
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

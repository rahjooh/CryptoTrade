package binance

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	liq "cryptoflow/internal/channel/liq"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Binance_LIQ_Reader streams liquidation orders from the Binance futures websocket.
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

// Binance_LIQ_Start launches websocket subscriptions for each configured symbol.
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
	log := r.log.WithComponent("binance_liq_reader").WithFields(logger.Fields{
		"operation": "Binance_LIQ_Start",
	})

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

	log.WithFields(logger.Fields{
		"symbols": strings.Join(r.symbols, ","),
	}).Info("starting binance liquidation reader")

	for _, symbol := range r.symbols {
		sym := strings.ToUpper(strings.TrimSpace(symbol))
		if sym == "" {
			continue
		}
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

	cfg := r.config.Source.Binance.Future.Liquidation
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		baseURL = "wss://fstream.binance.com"
	}

	for {
		if r.ctx.Err() != nil {
			return
		}

		streamURL := fmt.Sprintf("%s/ws/%s@forceOrder", baseURL, strings.ToLower(symbol))
		conn, _, err := websocket.DefaultDialer.DialContext(r.ctx, streamURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to binance liquidation websocket, retrying")
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		conn.SetReadDeadline(time.Now().Add(35 * time.Second))
		pingCtx, pingCancel := context.WithCancel(context.Background())
		pingTicker := time.NewTicker(20 * time.Second)

		conn.SetPongHandler(func(appData string) error {
			conn.SetReadDeadline(time.Now().Add(35 * time.Second))
			return nil
		})

		// ping goroutine
		go func() {
			defer pingTicker.Stop()
			for {
				select {
				case <-pingCtx.Done():
					return
				case <-pingTicker.C:
					conn.SetWriteDeadline(time.Now().Add(time.Second))
					if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
						log.WithError(err).Warn("failed to send binance ping")
						pingCancel()
						return
					}
				}
			}
		}()

	loop:
		for {
			if r.ctx.Err() != nil {
				_ = conn.Close()
				break loop
			}

			_, msg, err := conn.ReadMessage()
			if err != nil {
				_ = conn.Close()
				log.WithError(err).Warn("binance liquidation stream error, reconnecting")
				break loop
			}

			r.forwardMessage(msg, symbol, log)
		}

		pingCancel()
		select {
		case <-time.After(2 * time.Second):
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Binance_LIQ_Reader) forwardMessage(payload []byte, symbol string, log *logger.Entry) {
	data := append([]byte(nil), payload...)

	msg := models.RawLiquidation{
		Exchange: models.ExchangeBinance,
		Payload:  data,
	}

	if r.channels.SendRaw(r.ctx, msg) {
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
		}).Debug("forwarded binance liquidation event to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		metrics.EmitDropMetric(r.log, metrics.DropMetricLiquidationRaw, "binance", "liquidation", strings.ToUpper(symbol), "raw")
		log.Warn("liquidation raw channel full, dropping binance message")
	}
}

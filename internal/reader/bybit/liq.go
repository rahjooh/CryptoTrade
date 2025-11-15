package bybit

import (
	"context"
	"encoding/json"
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

// Bybit_LIQ_Reader streams liquidation orders from the Bybit futures websocket.
type Bybit_LIQ_Reader struct {
	config   *appconfig.Config
	channels *liq.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
}

// Bybit_LIQ_NewReader constructs a new Bybit liquidation reader.
func Bybit_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels, symbols []string) *Bybit_LIQ_Reader {
	return &Bybit_LIQ_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Bybit_LIQ_Start launches websocket subscriptions for each configured symbol.
func (r *Bybit_LIQ_Reader) Bybit_LIQ_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit liquidation reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.Liquidation
	log := r.log.WithComponent("bybit_liq_reader").WithFields(logger.Fields{
		"operation": "Bybit_LIQ_Start",
	})

	if !cfg.Enabled {
		log.Warn("bybit futures liquidation stream disabled via configuration")
		return fmt.Errorf("bybit futures liquidation stream disabled")
	}
	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			log.Warn("no symbols configured for bybit liquidation reader")
			return fmt.Errorf("no symbols configured for bybit liquidation reader")
		}
		r.symbols = cfg.Symbols
	}

	log.WithFields(logger.Fields{
		"symbols": strings.Join(r.symbols, ","),
	}).Info("starting bybit liquidation reader")

	for _, symbol := range r.symbols {
		sym := strings.ToUpper(strings.TrimSpace(symbol))
		if sym == "" {
			continue
		}
		r.wg.Add(1)
		go r.streamSymbol(sym)
	}

	log.Info("bybit liquidation reader started successfully")
	return nil
}

// Bybit_LIQ_Stop waits for all symbol workers to stop.
func (r *Bybit_LIQ_Reader) Bybit_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("bybit_liq_reader").Info("stopping bybit liquidation reader")
	r.wg.Wait()
	r.log.WithComponent("bybit_liq_reader").Info("bybit liquidation reader stopped")
}

func (r *Bybit_LIQ_Reader) streamSymbol(symbol string) {
	defer r.wg.Done()

	log := r.log.WithComponent("bybit_liq_reader").WithFields(logger.Fields{
		"symbol": symbol,
		"worker": "liquidation_stream",
	})

	cfg := r.config.Source.Bybit.Future.Liquidation
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		baseURL = "wss://stream.bybit.com/v5/public/linear"
	}

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := websocket.DefaultDialer.DialContext(r.ctx, baseURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to bybit liquidation websocket, retrying")
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		// subscribe to allLiquidation.<symbol>
		topic := fmt.Sprintf("allLiquidation.%s", symbol)
		subMsg := map[string]any{
			"op":   "subscribe",
			"args": []string{topic},
		}
		if err := conn.WriteJSON(subMsg); err != nil {
			log.WithError(err).Warn("failed to send bybit subscription, reconnecting")
			_ = conn.Close()
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

		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(35 * time.Second))
			return nil
		})

		go func() {
			defer pingTicker.Stop()
			for {
				select {
				case <-pingCtx.Done():
					return
				case <-pingTicker.C:
					conn.SetWriteDeadline(time.Now().Add(time.Second))
					if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
						log.WithError(err).Warn("failed to send bybit ping")
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
				log.WithError(err).Warn("bybit liquidation stream error, reconnecting")
				break loop
			}

			// quick filter: ensure this is our liquidation topic
			var probe struct {
				Topic string `json:"topic"`
			}
			if err := json.Unmarshal(msg, &probe); err != nil {
				log.WithError(err).Debug("failed to unmarshal bybit probe, skipping message")
				continue
			}
			if !strings.HasPrefix(probe.Topic, "allLiquidation.") {
				continue
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

func (r *Bybit_LIQ_Reader) forwardMessage(payload []byte, symbol string, log *logger.Entry) {
	data := append([]byte(nil), payload...)

	msg := models.RawLiquidation{
		Exchange: models.ExchangeBybit,
		Payload:  data,
	}

	if r.channels.SendRaw(r.ctx, msg) {
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
		}).Debug("forwarded bybit liquidation event to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		metrics.EmitDropMetric(r.log, metrics.DropMetricLiquidationRaw, "bybit", "liquidation", strings.ToUpper(symbol), "raw")
		log.Warn("liquidation raw channel full, dropping bybit message")
	}
}

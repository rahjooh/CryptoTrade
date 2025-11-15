package okx

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

type OKX_LIQ_Reader struct {
	config   *appconfig.Config
	channels *liq.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
}

// OKX_LIQ_NewReader constructs a new OKX liquidation reader (for SWAP).
func OKX_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels) *OKX_LIQ_Reader {
	return &OKX_LIQ_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
	}
}

// OKX_LIQ_Start launches the OKX liquidation-orders SWAP stream.
func (r *OKX_LIQ_Reader) OKX_LIQ_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("okx liquidation reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	// NOTE: using Swap, not Future
	cfg := r.config.Source.Okx.Future.Liquidation
	log := r.log.WithComponent("okx_liq_reader").WithFields(logger.Fields{
		"operation": "OKX_LIQ_Start",
	})

	if !cfg.Enabled {
		log.Warn("okx swap liquidation stream disabled via configuration")
		return fmt.Errorf("okx swap liquidation stream disabled")
	}

	log.Info("starting okx swap liquidation reader")

	r.wg.Add(1)
	go r.stream()

	log.Info("okx swap liquidation reader started successfully")
	return nil
}

// OKX_LIQ_Stop waits for the OKX worker to stop.
func (r *OKX_LIQ_Reader) OKX_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("okx_liq_reader").Info("stopping okx swap liquidation reader")
	r.wg.Wait()
	r.log.WithComponent("okx_liq_reader").Info("okx swap liquidation reader stopped")
}

func (r *OKX_LIQ_Reader) stream() {
	defer r.wg.Done()

	log := r.log.WithComponent("okx_liq_reader").WithFields(logger.Fields{
		"worker": "liquidation_orders_stream",
	})

	// NOTE: using Swap, not Future
	cfg := r.config.Source.Okx.Future.Liquidation
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		baseURL = "wss://ws.okx.com:8443/ws/v5/public"
	}

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := websocket.DefaultDialer.DialContext(r.ctx, baseURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to okx swap liquidation websocket, retrying")
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		// subscribe to liquidation-orders SWAP
		subMsg := map[string]any{
			"op": "subscribe",
			"args": []map[string]string{
				{
					"channel":  "liquidation-orders",
					"instType": "SWAP",
				},
			},
		}
		if err := conn.WriteJSON(subMsg); err != nil {
			log.WithError(err).Warn("failed to send okx swap subscription, reconnecting")
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
						log.WithError(err).Warn("failed to send okx swap ping")
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
				log.WithError(err).Warn("okx swap liquidation stream error, reconnecting")
				break loop
			}

			// filter only liquidation-orders SWAP data, ignore subscribe acks, pings, etc.
			var probe struct {
				Arg struct {
					Channel  string `json:"channel"`
					InstType string `json:"instType"`
				} `json:"arg"`
				Event string `json:"event"`
			}
			if err := json.Unmarshal(msg, &probe); err != nil {
				log.WithError(err).Debug("failed to unmarshal okx probe, skipping message")
				continue
			}
			if probe.Event != "" {
				continue // subscribe ack etc
			}
			if probe.Arg.Channel != "liquidation-orders" || probe.Arg.InstType != "SWAP" {
				continue
			}

			r.forwardMessage(msg, log)
		}

		pingCancel()
		select {
		case <-time.After(2 * time.Second):
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *OKX_LIQ_Reader) forwardMessage(payload []byte, log *logger.Entry) {
	data := append([]byte(nil), payload...)

	msg := models.RawLiquidation{
		Exchange: models.ExchangeOKX,
		Payload:  data,
	}

	if r.channels.SendRaw(r.ctx, msg) {
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
		}).Debug("forwarded okx swap liquidation event to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		metrics.EmitDropMetric(r.log, metrics.DropMetricLiquidationRaw, "okx", "liquidation", "", "raw")
		log.Warn("liquidation raw channel full, dropping okx swap message")
	}
}

package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/channel/foi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// Okx_FOI_Reader consumes SWAP open-interest data from the OKX public
// websocket and forwards raw FOI payloads into foi.raw.
//
// It subscribes with a single message for multiple instruments, e.g.:
//
//	{
//	  "id": "oi-sub-1",
//	  "op": "subscribe",
//	  "args": [
//	    { "channel": "open-interest", "instId": "BTC-USDT-SWAP" },
//	    { "channel": "open-interest", "instId": "ETH-USDT-SWAP" },
//	    ...
//	  ]
//	}
//
// The reader:
//
//   - Maintains a single websocket connection for all instIds.
//   - Automatically reconnects on errors.
//   - Sends only "open-interest" events that contain a non-empty "data"
//     array into foi.raw as models.RawFOI.
type Okx_FOI_Reader struct {
	config   *appconfig.Config
	channels *foi.Channels

	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool

	log     *logger.Log
	instIDs []string // e.g. ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
	ip      string
}

// Okx_FOI_NewReader constructs a new OKX FOI reader.
//
//   - cfg:     global config
//   - ch:      FOI channels
//   - instIDs: optional explicit instrument IDs; if empty, uses config symbols
//     from source.okx.future.open_interest.symbols
//   - localIP: optional outbound IP for sharding; currently unused
func Okx_FOI_NewReader(cfg *appconfig.Config, ch *foi.Channels, instIDs []string, localIP string) *Okx_FOI_Reader {
	log := logger.GetLogger()
	return &Okx_FOI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      log,
		instIDs:  instIDs,
		ip:       localIP,
	}
}

// Okx_FOI_Start launches the main websocket loop and begins consuming
// open-interest snapshots for all configured instruments.
func (r *Okx_FOI_Reader) Okx_FOI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("okx FOI reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.OpenInterest
	log := r.log.WithComponent("okx_FOI_reader").WithFields(logger.Fields{
		"operation": "Okx_FOI_Start",
	})

	if !cfg.Enabled {
		log.Warn("okx futures open_interest stream disabled via configuration")
		return fmt.Errorf("okx futures open_interest stream disabled")
	}
	if cfg.Connection != "websocket" {
		log.WithFields(logger.Fields{"connection": cfg.Connection}).
			Warn("okx FOI reader expects connection=websocket")
		return fmt.Errorf("okx FOI reader expects websocket connection")
	}

	// If instIDs were not supplied explicitly, derive them from config symbols.
	if len(r.instIDs) == 0 {
		r.instIDs = cfg.Symbols
	}
	if len(r.instIDs) == 0 {
		log.Warn("no instIds/symbols configured for okx FOI reader")
		return fmt.Errorf("no instIds configured for okx FOI reader")
	}

	log.WithFields(logger.Fields{
		"inst_ids": strings.Join(r.instIDs, ","),
		"url":      cfg.URL,
	}).Info("starting OKX FOI reader")

	r.wg.Add(1)
	go r.streamFOI()

	log.Info("OKX FOI reader started successfully")
	return nil
}

// Okx_FOI_Stop waits for the websocket loop to exit.
func (r *Okx_FOI_Reader) Okx_FOI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("okx_FOI_reader").Info("stopping OKX FOI reader")
	r.wg.Wait()
	r.log.WithComponent("okx_FOI_reader").Info("OKX FOI reader stopped")
}

// streamFOI manages the websocket connection, subscription, ping/pong loop,
// and message forwarding. It will reconnect on errors until the context
// is canceled.
func (r *Okx_FOI_Reader) streamFOI() {
	defer r.wg.Done()

	log := r.log.WithComponent("okx_FOI_reader").WithFields(logger.Fields{
		"worker": "foi_stream",
	})

	cfg := r.config.Source.Okx.Future.OpenInterest
	baseURL := strings.TrimSpace(cfg.URL)
	if baseURL == "" {
		baseURL = "wss://ws.okx.com:8443/ws/v5/public"
	}

	reconnectDelay := cfg.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = 5 * time.Second
	}

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := websocket.DefaultDialer.DialContext(r.ctx, baseURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to OKX FOI websocket, retrying")
			select {
			case <-time.After(reconnectDelay):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		log.Info("connected to OKX FOI websocket")

		// Build subscribe message for all configured instIds.
		type arg struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		}
		subMsg := struct {
			ID   string `json:"id"`
			Op   string `json:"op"`
			Args []arg  `json:"args"`
		}{
			ID: "oi-sub-1",
			Op: "subscribe",
		}
		for _, inst := range r.instIDs {
			subMsg.Args = append(subMsg.Args, arg{
				Channel: "open-interest",
				InstID:  inst,
			})
		}

		if err := conn.WriteJSON(subMsg); err != nil {
			log.WithError(err).Warn("failed to send OKX FOI subscribe message")
			conn.Close()
			select {
			case <-time.After(reconnectDelay):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		// Setup ping/pong and read deadlines.
		conn.SetReadDeadline(time.Now().Add(35 * time.Second))
		pingCtx, pingCancel := context.WithCancel(context.Background())
		pingTicker := time.NewTicker(20 * time.Second)
		conn.SetPongHandler(func(appData string) error {
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
						log.WithError(err).Warn("failed to send OKX ping")
						pingCancel()
						return
					}
				}
			}
		}()

	readLoop:
		for {
			if r.ctx.Err() != nil {
				conn.Close()
				break readLoop
			}

			_, msg, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				log.WithError(err).Warn("OKX FOI stream error, reconnecting")
				break readLoop
			}
			r.forwardMessage(msg, log)
		}

		pingCancel()
		select {
		case <-time.After(reconnectDelay):
		case <-r.ctx.Done():
			return
		}
	}
}

// forwardMessage inspects the incoming websocket payload and forwards only
// actual "open-interest" data messages into foi.raw.
//
// It filters out subscription acks and other control messages to avoid
// unnecessary decode errors in the processor.
func (r *Okx_FOI_Reader) forwardMessage(payload []byte, log *logger.Entry) {
	// Lightweight check before forwarding to processor.
	var peek struct {
		Arg struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &peek); err != nil {
		// If we can't decode, ignore silently here.
		return
	}
	if strings.ToLower(peek.Arg.Channel) != "open-interest" {
		// Not FOI, ignore.
		return
	}
	if len(peek.Data) == 0 {
		// Likely a subscription ack {event: "subscribe"} without data; ignore.
		return
	}

	if r.log.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logger.Fields{
			"channel": peek.Arg.Channel,
			"payload": string(peek.Data),
		}).Debug("decoded OKX FOI websocket payload")
	}

	data := json.RawMessage(append([]byte(nil), payload...))

	msg := models.RawFOI{
		Exchange: models.ExchangeOKX,
		Payload:  data,
	}

	if r.channels.SendRaw(r.ctx, msg) {
		log.WithFields(logger.Fields{
			"payload_bytes": len(payload),
		}).Debug("forwarded OKX FOI event to foi.raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "okx", "foi", "", "raw")
		log.Warn("FOI raw channel full, dropping OKX FOI message")
	}
}

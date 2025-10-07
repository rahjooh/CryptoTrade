package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobd "cryptoflow/internal/channel/fobd"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Okx_FOBD_Reader streams swap order book deltas from OKX's public websocket
// and forwards the raw messages to the configured channel. The implementation
// uses a plain websocket connection without relying on an SDK.
type Okx_FOBD_Reader struct {
	config   *appconfig.Config
	channels *fobd.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	localIP  string
}

// Okx_FOBD_NewReader creates a new delta reader. The localIP parameter is kept
// for compatibility; when provided, the websocket dialer binds to it.
func Okx_FOBD_NewReader(cfg *appconfig.Config, ch *fobd.Channels, symbols []string, localIP string) *Okx_FOBD_Reader {
	return &Okx_FOBD_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Okx_FOBD_Start establishes a websocket connection to the OKX public
// endpoint and subscribes to swap order book deltas for all configured symbols.
// The connection is automatically re-established until the context is
// cancelled.
func (r *Okx_FOBD_Reader) Okx_FOBD_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("Okx_FOBD_Reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.Orderbook.Delta
	log := r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"operation": "Okx_FOBD_Start"})
	if !cfg.Enabled {
		log.Warn("okx swap orderbook delta is disabled")
		return fmt.Errorf("okx swap orderbook delta is disabled")
	}

	wsURL := cfg.URL
	if wsURL == "" {
		wsURL = "wss://ws.okx.com:8443/ws/v5/public"
	}

	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting okx swap delta reader")
	r.wg.Add(1)
	go r.stream(r.symbols, wsURL)
	log.Info("okx swap delta reader started successfully")
	return nil
}

// Okx_FOBD_Stop terminates all websocket subscriptions and waits for workers to
// finish.
func (r *Okx_FOBD_Reader) Okx_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
	r.log.WithComponent("okx_delta_reader").Info("stopping okx delta reader")
	r.wg.Wait()
	r.log.WithComponent("okx_delta_reader").Info("okx delta reader stopped")
}

// stream manages the websocket lifecycle and message processing.
func (r *Okx_FOBD_Reader) stream(symbols []string, wsURL string) {
	defer r.wg.Done()
	log := r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"symbols": symbols, "worker": "delta_stream"})

	for {
		if r.ctx.Err() != nil {
			return
		}
		dialer := websocket.Dialer{}
		if r.localIP != "" {
			if ip := net.ParseIP(r.localIP); ip != nil {
				dialer.NetDialContext = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}).DialContext
			}
		}

		log.WithField("url", wsURL).Debug("connecting to websocket")
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect websocket, retrying")
			// log will capture retry attempts for visibility
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}
		log.Info("websocket connected")

		args := make([]map[string]string, 0, len(symbols))
		for _, sym := range symbols {
			args = append(args, map[string]string{
				"channel":  "books",
				"instType": "SWAP",
				"instId":   sym,
			})
		}
		sub := map[string]any{"op": "subscribe", "args": args}
		log.WithField("request", sub).Info("sending subscription request")
		if err := conn.WriteJSON(sub); err != nil {
			log.WithError(err).Warn("failed to subscribe")
			conn.Close()
			continue
		}
		// Read subscription acknowledgement
		if _, resp, err := conn.ReadMessage(); err != nil {
			log.WithError(err).Warn("failed to read subscription response")
			conn.Close()
			continue
		} else {
			log.WithField("response", string(resp)).Debug("subscription response received")
			r.processMessage(conn, resp)
		}

		pingTicker := time.NewTicker(20 * time.Second)
		done := make(chan struct{})
		go func() {
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					conn.WriteMessage(websocket.PingMessage, nil)
				}
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				close(done)
				conn.Close()
				log.WithError(err).Warn("websocket read error, reconnecting")
				break
			}
			log.WithField("raw", string(msg)).Debug("received websocket message")
			r.processMessage(conn, msg)
		}

		time.Sleep(time.Second)
	}
}

// processMessage handles ping/pong frames and forwards order book updates.
func (r *Okx_FOBD_Reader) processMessage(conn *websocket.Conn, msg []byte) bool {
	var base map[string]json.RawMessage
	if err := json.Unmarshal(msg, &base); err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Debug("failed to decode message")
		return false
	}
	if _, ok := base["event"]; ok {
		var evt struct {
			Event string `json:"event"`
		}
		json.Unmarshal(msg, &evt)
		r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"event": evt.Event, "raw": string(msg)}).Debug("received event message")
		if evt.Event == "ping" && conn != nil {
			conn.WriteMessage(websocket.TextMessage, []byte("{\"op\":\"pong\"}"))
		}
		return false
	}
	if _, ok := base["ping"]; ok {
		var ping struct {
			Ping int64 `json:"ping"`
		}
		if err := json.Unmarshal(msg, &ping); err == nil && conn != nil {
			r.log.WithComponent("okx_delta_reader").WithField("ping", ping.Ping).Debug("received ping frame")
			resp, _ := json.Marshal(map[string]int64{"pong": ping.Ping})
			conn.WriteMessage(websocket.TextMessage, resp)
		}
		return false
	}

	var evt orderBookEvent
	if err := json.Unmarshal(msg, &evt); err != nil {
		return false
	}
	r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"symbol": evt.Arg.InstID, "action": evt.Action}).Debug("processing orderbook event")
	r.handleEvent(&evt)
	return true
}

type orderBookEvent struct {
	Arg struct {
		InstID string `json:"instId"`
	} `json:"arg"`
	Action string `json:"action"`
	Data   []struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Ts   string     `json:"ts"`
	} `json:"data"`
}

// handleEvent converts a websocket event into a raw delta message.
func (r *Okx_FOBD_Reader) handleEvent(evt *orderBookEvent) {
	if evt == nil || len(evt.Data) == 0 {
		return
	}
	book := evt.Data[0]
	bids := make([]models.FOBDEntry, len(book.Bids))
	for i, b := range book.Bids {
		if len(b) >= 2 {
			bids[i] = models.FOBDEntry{Price: b[0], Quantity: b[1]}
		}
	}
	asks := make([]models.FOBDEntry, len(book.Asks))
	for i, a := range book.Asks {
		if len(a) >= 2 {
			asks[i] = models.FOBDEntry{Price: a[0], Quantity: a[1]}
		}
	}
	ts, _ := strconv.ParseInt(book.Ts, 10, 64)
	resp := models.OkxFOBDResp{
		Symbol:    evt.Arg.InstID,
		Action:    evt.Action,
		Timestamp: ts,
		Bids:      bids,
		Asks:      asks,
	}

	payload, err := json.Marshal(resp)
	if err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Warn("failed to marshal event")
		return
	}
	msg := models.RawFOBDMessage{
		Exchange:  "okx",
		Symbol:    evt.Arg.InstID,
		Market:    "swap-orderbook-delta",
		Data:      payload,
		Timestamp: time.Now(),
	}
	r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{
		"symbol": resp.Symbol,
		"action": resp.Action,
		"bids":   len(resp.Bids),
		"asks":   len(resp.Asks),
	}).Debug("forwarding delta to channel")
	if r.channels.SendRaw(r.ctx, msg) {
		r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{
			"payload_bytes": len(payload),
			"symbol":        resp.Symbol,
		}).Debug("delta message forwarded to raw channel")
	} else if r.ctx.Err() != nil {
		return
	} else {
		r.log.WithComponent("okx_delta_reader").Warn("raw delta channel full, dropping message")
	}
}

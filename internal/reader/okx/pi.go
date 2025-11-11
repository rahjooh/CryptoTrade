package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	pichannel "cryptoflow/internal/channel/pi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Okx_PI_Reader streams premium-index (mark price) updates from OKX websockets.
type Okx_PI_Reader struct {
	config   *appconfig.Config
	channels *pichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	localIP  string
	instType string
}

// Okx_PI_NewReader builds a new reader.
func Okx_PI_NewReader(cfg *appconfig.Config, ch *pichannel.Channels, symbols []string, localIP string) *Okx_PI_Reader {
	return &Okx_PI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Okx_PI_Start connects to OKX public websocket.
func (r *Okx_PI_Reader) Okx_PI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("okx premium-index reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.PremiumIndex
	if !cfg.Enabled {
		return fmt.Errorf("okx premium-index disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for okx premium-index reader")
		}
		r.symbols = cfg.Symbols
	}
	r.instType = cfg.InstType
	if r.instType == "" {
		r.instType = "SWAP"
	}

	r.wg.Add(1)
	go r.stream(cfg)

	r.log.WithComponent("okx_pi_reader").WithFields(logger.Fields{
		"symbols":  r.symbols,
		"instType": r.instType,
	}).Info("okx premium-index reader started")
	return nil
}

// Okx_PI_Stop stops the websocket worker.
func (r *Okx_PI_Reader) Okx_PI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("okx_pi_reader").Info("stopping okx premium-index reader")
	r.wg.Wait()
	r.log.WithComponent("okx_pi_reader").Info("okx premium-index reader stopped")
}

func (r *Okx_PI_Reader) stream(cfg appconfig.PremiumIndexConfig) {
	defer r.wg.Done()

	wsURL := cfg.URL
	if wsURL == "" {
		wsURL = "wss://ws.okx.com:8443/ws/v5/public"
	}

	reconnect := cfg.ReconnectDelay
	if reconnect <= 0 {
		reconnect = 5 * time.Second
	}

	dialer := websocket.Dialer{}
	if r.localIP != "" {
		if ip := net.ParseIP(r.localIP); ip != nil {
			dialer.NetDialContext = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}).DialContext
		}
	}

	log := r.log.WithComponent("okx_pi_reader")

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to okx premium-index websocket")
			select {
			case <-time.After(reconnect):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		args := make([]map[string]string, 0, len(r.symbols))
		for _, sym := range r.symbols {
			args = append(args, map[string]string{
				"channel":  "mark-price",
				"instType": r.instType,
				"instId":   sym,
			})
		}
		req := map[string]any{"op": "subscribe", "args": args}
		if err := conn.WriteJSON(req); err != nil {
			log.WithError(err).Warn("failed to subscribe to okx mark-price channel")
			conn.Close()
			continue
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				log.WithError(err).Warn("okx premium-index stream error, reconnecting")
				break
			}
			if !r.handleMessage(raw) {
				continue
			}
		}

		select {
		case <-time.After(reconnect):
		case <-r.ctx.Done():
			return
		}
	}
}

type okxMarkPricePayload struct {
	Arg struct {
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
		InstType string `json:"instType"`
	} `json:"arg"`
	Data []struct {
		InstID     string `json:"instId"`
		MarkPrice  string `json:"markPx"`
		IndexPrice string `json:"idxPx"`
		Timestamp  string `json:"ts"`
	} `json:"data"`
	Event string `json:"event"`
}

func (r *Okx_PI_Reader) handleMessage(raw []byte) bool {
	var payload okxMarkPricePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.log.WithComponent("okx_pi_reader").WithError(err).Debug("failed to decode okx premium-index payload")
		return false
	}

	if payload.Event != "" {
		return false
	}
	if payload.Arg.Channel != "mark-price" {
		return false
	}

	for _, entry := range payload.Data {
		mark := parseNumber(entry.MarkPrice)
		index := parseNumber(entry.IndexPrice)
		ts := time.Now().UTC()
		if entry.Timestamp != "" {
			if parsed, err := strconv.ParseInt(entry.Timestamp, 10, 64); err == nil {
				ts = time.UnixMilli(parsed).UTC()
			}
		}

		msg := models.RawPIMessage{
			Exchange:     "okx",
			Symbol:       strings.ToUpper(entry.InstID),
			Market:       "pi",
			MarkPrice:    mark,
			IndexPrice:   index,
			PremiumIndex: mark - index,
			Source:       "okx_ws",
			Timestamp:    ts,
			Payload:      append([]byte(nil), raw...),
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return false
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, "okx", "pi", msg.Symbol, "raw")
		}
	}
	return true
}

func parseNumber(v string) float64 {
	if v == "" {
		return 0
	}
	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return val
}

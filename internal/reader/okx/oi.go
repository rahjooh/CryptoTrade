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
	oichannel "cryptoflow/internal/channel/oi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Okx_OI_Reader consumes open-interest updates from the OKX websocket.
type Okx_OI_Reader struct {
	config   *appconfig.Config
	channels *oichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	localIP  string
	instType string
}

// Okx_OI_NewReader creates the reader with the provided symbol set.
func Okx_OI_NewReader(cfg *appconfig.Config, ch *oichannel.Channels, symbols []string, localIP string) *Okx_OI_Reader {
	return &Okx_OI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Okx_OI_Start connects to the websocket and subscribes to open-interest channels.
func (r *Okx_OI_Reader) Okx_OI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("okx open-interest reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.OpenInterest
	if !cfg.Enabled {
		return fmt.Errorf("okx open-interest disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for okx open-interest reader")
		}
		r.symbols = cfg.Symbols
	}
	r.instType = cfg.InstType
	if r.instType == "" {
		r.instType = "SWAP"
	}

	r.wg.Add(1)
	go r.stream(cfg)

	r.log.WithComponent("okx_oi_reader").WithFields(logger.Fields{
		"symbols":  r.symbols,
		"instType": r.instType,
	}).Info("okx open-interest reader started")
	return nil
}

// Okx_OI_Stop waits for stream goroutine to exit.
func (r *Okx_OI_Reader) Okx_OI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("okx_oi_reader").Info("stopping okx open-interest reader")
	r.wg.Wait()
	r.log.WithComponent("okx_oi_reader").Info("okx open-interest reader stopped")
}

func (r *Okx_OI_Reader) stream(cfg appconfig.OpenInterestConfig) {
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

	log := r.log.WithComponent("okx_oi_reader")

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to okx open-interest websocket")
			select {
			case <-time.After(reconnect):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		subs := make([]map[string]string, 0, len(r.symbols))
		for _, sym := range r.symbols {
			subs = append(subs, map[string]string{
				"channel":  "open-interest",
				"instType": r.instType,
				"instId":   sym,
			})
		}
		req := map[string]any{"op": "subscribe", "args": subs}
		if err := conn.WriteJSON(req); err != nil {
			log.WithError(err).Warn("failed to subscribe to okx open-interest channel")
			conn.Close()
			continue
		}

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				log.WithError(err).Warn("okx open-interest stream error, reconnecting")
				break
			}
			if !r.processMessage(msg) {
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

type okxOpenInterestPayload struct {
	Arg struct {
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
		InstType string `json:"instType"`
	} `json:"arg"`
	Data []struct {
		InstID string `json:"instId"`
		OI     string `json:"oi"`
		OICcy  string `json:"oiCcy"`
		Ts     string `json:"ts"`
	} `json:"data"`
	Event string `json:"event"`
}

func (r *Okx_OI_Reader) processMessage(raw []byte) bool {
	var payload okxOpenInterestPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.log.WithComponent("okx_oi_reader").WithError(err).Debug("failed to decode okx open-interest payload")
		return false
	}

	if payload.Event != "" {
		return false
	}
	if payload.Arg.Channel != "open-interest" {
		return false
	}

	for _, entry := range payload.Data {
		value, _ := strconv.ParseFloat(entry.OI, 64)
		valueCcy, _ := strconv.ParseFloat(entry.OICcy, 64)
		ts := time.Now().UTC()
		if entry.Ts != "" {
			if parsed, err := strconv.ParseInt(entry.Ts, 10, 64); err == nil {
				ts = time.UnixMilli(parsed).UTC()
			}
		}
		currency := payload.Arg.InstType
		if currency == "" {
			currency = "USD"
		}

		msg := models.RawOIMessage{
			Exchange:  "okx",
			Symbol:    strings.ToUpper(entry.InstID),
			Market:    "oi",
			Value:     value,
			ValueUSD:  valueCcy,
			Currency:  currency,
			Source:    "okx_ws",
			Timestamp: ts,
			Payload:   append([]byte(nil), raw...),
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return false
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "okx", "oi", msg.Symbol, "raw")
		}
	}
	return true
}

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
	liq "cryptoflow/internal/channel/liq"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Okx_LIQ_Reader streams liquidation orders from OKX public websocket.
type Okx_LIQ_Reader struct {
	config    *appconfig.Config
	channels  *liq.Channels
	ctx       context.Context
	wg        *sync.WaitGroup
	mu        sync.RWMutex
	running   bool
	log       *logger.Log
	symbols   []string
	symbolSet map[string]struct{}
	localIP   string
}

// Okx_LIQ_NewReader creates a new liquidation reader instance.
func Okx_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels, symbols []string, localIP string) *Okx_LIQ_Reader {
	return &Okx_LIQ_Reader{
		config:    cfg,
		channels:  ch,
		wg:        &sync.WaitGroup{},
		log:       logger.GetLogger(),
		symbols:   symbols,
		symbolSet: make(map[string]struct{}),
		localIP:   localIP,
	}
}

// Okx_LIQ_Start begins streaming liquidation orders.
func (r *Okx_LIQ_Reader) Okx_LIQ_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("okx liquidation reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.Liquidation
	log := r.log.WithComponent("okx_liq_reader").WithFields(logger.Fields{"operation": "Okx_LIQ_Start"})

	if !cfg.Enabled {
		log.Warn("okx liquidation stream disabled via configuration")
		return fmt.Errorf("okx liquidation stream disabled")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			log.Warn("no symbols configured for okx liquidation reader")
			return fmt.Errorf("no symbols configured for okx liquidation reader")
		}
		r.symbols = cfg.Symbols
	}

	for _, sym := range r.symbols {
		r.symbolSet[strings.ToUpper(sym)] = struct{}{}
	}

	wsURL := cfg.URL
	if wsURL == "" {
		wsURL = "wss://ws.okx.com:8443/ws/v5/public"
	}

	r.wg.Add(1)
	go r.stream(wsURL)
	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("okx liquidation reader started")
	return nil
}

// Okx_LIQ_Stop waits for the stream goroutine to end.
func (r *Okx_LIQ_Reader) Okx_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("okx_liq_reader").Info("stopping okx liquidation reader")
	r.wg.Wait()
	r.log.WithComponent("okx_liq_reader").Info("okx liquidation reader stopped")
}

func (r *Okx_LIQ_Reader) stream(wsURL string) {
	defer r.wg.Done()
	log := r.log.WithComponent("okx_liq_reader").WithFields(logger.Fields{"worker": "liq_stream"})

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

		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to okx websocket, retrying")
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		subs := make([]map[string]string, 0, len(r.symbols))
		for _, sym := range r.symbols {
			subs = append(subs, map[string]string{
				"channel":  "liquidation-orders",
				"instType": "SWAP",
				"instId":   sym,
			})
		}
		req := map[string]any{"op": "subscribe", "args": subs}
		if err := conn.WriteJSON(req); err != nil {
			log.WithError(err).Warn("failed to subscribe to okx liquidation channel")
			conn.Close()
			continue
		}

		conn.SetReadDeadline(time.Time{})

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				log.WithError(err).Warn("okx liquidation stream error, reconnecting")
				break
			}
			if !r.processMessage(msg) {
				continue
			}
		}

		select {
		case <-time.After(2 * time.Second):
		case <-r.ctx.Done():
			return
		}
	}
}

type okxLiquidationPayload struct {
	Arg struct {
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
		InstType string `json:"instType"`
	} `json:"arg"`
	Data []struct {
		InstID    string `json:"instId"`
		Uly       string `json:"uly"`
		Side      string `json:"side"`
		Position  string `json:"posSide"`
		Size      string `json:"sz"`
		Price     string `json:"px"`
		Timestamp string `json:"ts"`
	} `json:"data"`
	Event string `json:"event"`
}

func (r *Okx_LIQ_Reader) processMessage(raw []byte) bool {
	var payload okxLiquidationPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.log.WithComponent("okx_liq_reader").WithError(err).Debug("failed to decode okx message")
		return false
	}

	if payload.Event != "" {
		// subscription acknowledgement
		return false
	}

	if payload.Arg.Channel != "liquidation-orders" {
		return false
	}

	baseSymbol := strings.ToUpper(payload.Arg.InstID)
	for _, entry := range payload.Data {
		symbol := baseSymbol
		if entry.InstID != "" {
			symbol = strings.ToUpper(entry.InstID)
		}
		if len(r.symbolSet) > 0 {
			if _, ok := r.symbolSet[symbol]; !ok {
				continue
			}
		}

		msgPayload := struct {
			Arg  okxLiquidationPayload `json:"arg"`
			Data any                   `json:"data"`
		}{
			Arg:  payload,
			Data: entry,
		}
		// prevent recursion by clearing data slice
		msgPayload.Arg.Data = nil

		rawMsg, err := json.Marshal(msgPayload)
		if err != nil {
			r.log.WithComponent("okx_liq_reader").WithError(err).Warn("failed to marshal okx liquidation entry")
			continue
		}

		ts := time.Now().UTC()
		if entry.Timestamp != "" {
			if parsed, err := parseTimestamp(entry.Timestamp); err == nil {
				ts = parsed
			}
		}

		msg := models.RawLiquidationMessage{
			Exchange:  "okx",
			Symbol:    symbol,
			Market:    "liquidation",
			Data:      rawMsg,
			Timestamp: ts,
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return false
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricLiquidationRaw, "okx", "liquidation", symbol, "raw")
			r.log.WithComponent("okx_liq_reader").WithFields(logger.Fields{"symbol": symbol}).Warn("liquidation raw channel full, dropping message")
		}
	}
	return true
}

func parseTimestamp(value string) (time.Time, error) {
	if len(value) == 0 {
		return time.Now().UTC(), fmt.Errorf("empty timestamp")
	}
	if len(value) <= 10 {
		if sec, err := time.ParseDuration(value + "s"); err == nil {
			return time.Unix(0, sec.Nanoseconds()).UTC(), nil
		}
	}
	if i, err := parseInt(value); err == nil {
		switch {
		case i < 1_000_000_000_000:
			return time.Unix(i, 0).UTC(), nil
		case i < 1_000_000_000_000_000:
			return time.UnixMilli(i).UTC(), nil
		default:
			return time.Unix(0, i).UTC(), nil
		}
	}
	return time.Now().UTC(), fmt.Errorf("unable to parse timestamp")
}

func parseInt(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

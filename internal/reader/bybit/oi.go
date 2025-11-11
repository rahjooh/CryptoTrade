package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	oichannel "cryptoflow/internal/channel/oi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	bybit_connector "github.com/bybit-exchange/bybit.go.api"
)

// Bybit_OI_Reader streams open interest data from Bybit public websockets.
type Bybit_OI_Reader struct {
	config    *appconfig.Config
	channels  *oichannel.Channels
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	running   bool
	log       *logger.Log
	symbols   []string
	category  string
	symbolSet map[string]struct{}
	ws        *bybit_connector.WebSocket
}

// Bybit_OI_NewReader builds a new reader instance.
func Bybit_OI_NewReader(cfg *appconfig.Config, ch *oichannel.Channels, symbols []string) *Bybit_OI_Reader {
	return &Bybit_OI_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Bybit_OI_Start establishes the websocket connection and subscriptions.
func (r *Bybit_OI_Reader) Bybit_OI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit open-interest reader already running")
	}
	r.running = true
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.OpenInterest
	if !cfg.Enabled {
		return fmt.Errorf("bybit open-interest disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for bybit open-interest reader")
		}
		r.symbols = cfg.Symbols
	}

	r.category = strings.TrimSpace(cfg.Category)
	if r.category == "" {
		r.category = "linear"
	}

	r.symbolSet = make(map[string]struct{}, len(r.symbols))
	args := make([]string, 0, len(r.symbols))
	for _, sym := range r.symbols {
		symbol := strings.ToUpper(sym)
		r.symbolSet[symbol] = struct{}{}
		args = append(args, fmt.Sprintf("tickers.%s.%s", r.category, symbol))
	}

	wsURL := cfg.URL
	if wsURL == "" {
		wsURL = "wss://stream.bybit.com/v5/public/linear"
	}

	handler := func(message string) error {
		return r.handleMessage(message)
	}

	ws := bybit_connector.NewBybitPublicWebSocket(wsURL, handler)
	if ws == nil {
		return fmt.Errorf("failed to create bybit websocket client")
	}

	if ws.Connect() == nil {
		return fmt.Errorf("failed to connect to bybit websocket")
	}

	if _, err := ws.SendSubscription(args); err != nil {
		return fmt.Errorf("failed to subscribe to bybit tickers: %w", err)
	}

	r.ws = ws
	go r.monitorContext()

	r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{
		"symbols":  r.symbols,
		"category": r.category,
	}).Info("bybit open-interest reader started")
	return nil
}

// Bybit_OI_Stop disconnects the websocket and cancels background workers.
func (r *Bybit_OI_Reader) Bybit_OI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	ws := r.ws
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ws != nil {
		ws.Disconnect()
	}
	r.log.WithComponent("bybit_oi_reader").Info("bybit open-interest reader stopped")
}

func (r *Bybit_OI_Reader) monitorContext() {
	<-r.ctx.Done()
	r.Bybit_OI_Stop()
}

type bybitTickerPayload struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  []struct {
		Symbol            string `json:"symbol"`
		OpenInterest      string `json:"openInterest"`
		OpenInterestValue string `json:"openInterestValue"`
		Timestamp         string `json:"timestamp"`
	} `json:"data"`
}

func (r *Bybit_OI_Reader) handleMessage(raw string) error {
	var ack struct {
		Op      string `json:"op"`
		Success bool   `json:"success"`
		RetMsg  string `json:"ret_msg"`
	}
	if err := json.Unmarshal([]byte(raw), &ack); err == nil && ack.Op != "" {
		if !ack.Success {
			r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{
				"op":      ack.Op,
				"message": ack.RetMsg,
			}).Warn("subscription acknowledgement failure")
		}
		return nil
	}

	var payload bybitTickerPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	if !strings.HasPrefix(payload.Topic, "tickers") {
		return nil
	}

	for _, entry := range payload.Data {
		symbol := strings.ToUpper(entry.Symbol)
		if len(r.symbolSet) > 0 {
			if _, ok := r.symbolSet[symbol]; !ok {
				continue
			}
		}

		entryTime := time.Now().UTC()
		if payload.Ts > 0 {
			entryTime = time.UnixMilli(payload.Ts).UTC()
		}

		value, _ := strconv.ParseFloat(entry.OpenInterest, 64)
		valueUSD, _ := strconv.ParseFloat(entry.OpenInterestValue, 64)
		if entry.Timestamp != "" {
			if ts, err := strconv.ParseInt(entry.Timestamp, 10, 64); err == nil {
				entryTime = time.UnixMilli(ts).UTC()
			}
		}

		msg := models.RawOIMessage{
			Exchange:  "bybit",
			Symbol:    symbol,
			Market:    "oi",
			Value:     value,
			ValueUSD:  valueUSD,
			Source:    fmt.Sprintf("bybit_%s_ws", r.category),
			Timestamp: entryTime,
			Payload:   append([]byte(nil), []byte(raw)...),
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "bybit", "oi", symbol, "raw")
			r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{
				"symbol": symbol,
			}).Warn("dropping bybit open-interest message due to saturated channel")
		}
	}
	return nil
}

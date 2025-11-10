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
	liq "cryptoflow/internal/channel/liq"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	bybit_connector "github.com/bybit-exchange/bybit.go.api"
)

// Bybit_LIQ_Reader consumes liquidation updates from Bybit public websocket.
type Bybit_LIQ_Reader struct {
	config    *appconfig.Config
	channels  *liq.Channels
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	running   bool
	log       *logger.Log
	symbols   []string
	symbolSet map[string]struct{}
	ws        *bybit_connector.WebSocket
}

// Bybit_LIQ_NewReader builds a new reader instance.
func Bybit_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels, symbols []string) *Bybit_LIQ_Reader {
	return &Bybit_LIQ_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Bybit_LIQ_Start starts the websocket connection and subscribes to
// liquidation topics for configured symbols.
func (r *Bybit_LIQ_Reader) Bybit_LIQ_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit liquidation reader already running")
	}
	r.running = true
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.Liquidation
	log := r.log.WithComponent("bybit_liq_reader").WithFields(logger.Fields{"operation": "Bybit_LIQ_Start"})

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

	r.symbolSet = make(map[string]struct{}, len(r.symbols))
	topics := make([]string, 0, len(r.symbols))
	for _, sym := range r.symbols {
		symbol := strings.ToUpper(sym)
		r.symbolSet[symbol] = struct{}{}
		topics = append(topics, fmt.Sprintf("liquidation.linear.%s", symbol))
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
		log.Error("failed to create bybit websocket client")
		return fmt.Errorf("failed to create bybit websocket client")
	}

	if ws.Connect() == nil {
		log.Error("failed to connect to bybit websocket")
		return fmt.Errorf("failed to connect to bybit websocket")
	}

	if _, err := ws.SendSubscription(topics); err != nil {
		log.WithError(err).Error("failed to subscribe to bybit liquidation topics")
		return fmt.Errorf("failed to subscribe to bybit liquidation topics: %w", err)
	}

	r.ws = ws
	log.WithFields(logger.Fields{"topics": topics}).Info("bybit liquidation reader started")

	go r.monitorContext()
	return nil
}

// Bybit_LIQ_Stop disconnects the websocket and cancels background goroutines.
func (r *Bybit_LIQ_Reader) Bybit_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	ws := r.ws
	r.mu.Unlock()

	r.log.WithComponent("bybit_liq_reader").Info("stopping bybit liquidation reader")
	if cancel != nil {
		cancel()
	}
	if ws != nil {
		ws.Disconnect()
	}
	r.log.WithComponent("bybit_liq_reader").Info("bybit liquidation reader stopped")
}

func (r *Bybit_LIQ_Reader) monitorContext() {
	<-r.ctx.Done()
	r.Bybit_LIQ_Stop()
}

type bybitLiquidationPayload struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  []struct {
		Symbol      string `json:"symbol"`
		Side        string `json:"side"`
		Size        string `json:"size"`
		Price       string `json:"price"`
		UpdatedTime string `json:"updatedTime"`
		ExecQty     string `json:"execQty"`
		ExecPrice   string `json:"execPrice"`
	} `json:"data"`
}

func (r *Bybit_LIQ_Reader) handleMessage(msg string) error {
	var ack struct {
		Op      string `json:"op"`
		Success bool   `json:"success"`
		RetMsg  string `json:"ret_msg"`
	}
	if err := json.Unmarshal([]byte(msg), &ack); err == nil && ack.Op != "" {
		if !ack.Success {
			r.log.WithComponent("bybit_liq_reader").WithFields(logger.Fields{"operation": ack.Op, "message": ack.RetMsg}).Warn("subscription acknowledgement failure")
		}
		return nil
	}

	var payload bybitLiquidationPayload
	if err := json.Unmarshal([]byte(msg), &payload); err != nil {
		r.log.WithComponent("bybit_liq_reader").WithError(err).Debug("failed to decode bybit message")
		return nil
	}

	if !strings.HasPrefix(payload.Topic, "liquidation") {
		return nil
	}

	baseTime := time.Now().UTC()
	if payload.Ts > 0 {
		baseTime = time.UnixMilli(payload.Ts).UTC()
	}

	for _, entry := range payload.Data {
		symbol := strings.ToUpper(entry.Symbol)
		if len(r.symbolSet) > 0 {
			if _, ok := r.symbolSet[symbol]; !ok {
				continue
			}
		}

		eventTime := baseTime
		if entry.UpdatedTime != "" {
			if ts, err := strconv.ParseInt(entry.UpdatedTime, 10, 64); err == nil {
				eventTime = time.UnixMilli(ts).UTC()
			}
		}

		wrapped := map[string]any{
			"topic": payload.Topic,
			"type":  payload.Type,
			"ts":    payload.Ts,
			"data":  entry,
		}
		raw, err := json.Marshal(wrapped)
		if err != nil {
			r.log.WithComponent("bybit_liq_reader").WithError(err).Warn("failed to marshal liquidation entry")
			continue
		}

		msg := models.RawLiquidationMessage{
			Exchange:  "bybit",
			Symbol:    symbol,
			Market:    "liquidation",
			Data:      raw,
			Timestamp: eventTime,
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return nil
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricLiquidationRaw, "bybit", "liquidation", symbol, "raw")
			r.log.WithComponent("bybit_liq_reader").WithFields(logger.Fields{"symbol": symbol}).Warn("liquidation raw channel full, dropping message")
		}
	}

	return nil
}

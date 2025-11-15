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

	"github.com/gorilla/websocket"
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
	wsConn    *websocket.Conn
	connMu    sync.Mutex
	wg        sync.WaitGroup
}

func Bybit_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels, symbols []string) *Bybit_LIQ_Reader {
	return &Bybit_LIQ_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Start the websocket and subscribe to allLiquidation topics.
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

	// Symbols
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
		s := strings.ToUpper(strings.TrimSpace(sym))
		if s == "" {
			continue
		}
		r.symbolSet[s] = struct{}{}
		// ✅ v5 "All Liquidations" topic (symbol-scoped). Do NOT prefix with ".linear".
		// Category (linear/inverse/spot/option) is determined by the endpoint URL.
		topics = append(topics, fmt.Sprintf("allLiquidation.%s", s))
	}

	// Endpoint: linear (USDT/USDC perps). Change if you need inverse/spot/options.
	wsURL := cfg.URL
	if wsURL == "" {
		wsURL = "wss://stream.bybit.com/v5/public/linear"
	}

	reconnectDelay := cfg.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = 5 * time.Second
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		runBybitWebSocket(r.ctx, wsURL, topics, reconnectDelay, log, r.handleMessage, r.trackConn)
	}()

	log.WithFields(logger.Fields{"topics": topics, "endpoint": wsURL}).Info("bybit liquidation reader started")
	go r.monitorContext()
	return nil
}

func (r *Bybit_LIQ_Reader) Bybit_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	r.mu.Unlock()

	r.log.WithComponent("bybit_liq_reader").Info("stopping bybit liquidation reader")
	if cancel != nil {
		cancel()
	}
	r.closeActiveConn()
	r.wg.Wait()
	r.log.WithComponent("bybit_liq_reader").Info("bybit liquidation reader stopped")
}

func (r *Bybit_LIQ_Reader) monitorContext() {
	<-r.ctx.Done()
	r.Bybit_LIQ_Stop()
}

// Payload for (all) liquidation stream
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
	// Subscription ack (success/failure)
	var ack struct {
		Op      string `json:"op"`
		Success bool   `json:"success"`
		RetMsg  string `json:"ret_msg"`
	}
	if err := json.Unmarshal([]byte(msg), &ack); err == nil && ack.Op != "" {
		if !ack.Success {
			lf := r.log.WithComponent("bybit_liq_reader").WithFields(
				logger.Fields{"operation": ack.Op, "message": ack.RetMsg},
			)
			// Helpful hint for common topic mistake
			if strings.Contains(ack.RetMsg, "handler not found") &&
				strings.Contains(ack.RetMsg, "liquidation.") {
				lf.Warn("subscription acknowledgement failure (did you mean allLiquidation.<SYMBOL> and the /v5/public/linear endpoint?)")
			} else {
				lf.Warn("subscription acknowledgement failure")
			}
		}
		return nil
	}

	// Data payload
	var payload bybitLiquidationPayload
	if err := json.Unmarshal([]byte(msg), &payload); err != nil {
		r.log.WithComponent("bybit_liq_reader").WithError(err).Debug("failed to decode bybit message")
		return nil
	}
	// Only handle liquidation topics
	if !strings.HasPrefix(payload.Topic, "allLiquidation") && !strings.HasPrefix(payload.Topic, "liquidation") {
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

func (r *Bybit_LIQ_Reader) trackConn(conn *websocket.Conn) {
	r.connMu.Lock()
	r.wsConn = conn
	r.connMu.Unlock()
}

func (r *Bybit_LIQ_Reader) closeActiveConn() {
	r.connMu.Lock()
	conn := r.wsConn
	r.wsConn = nil
	r.connMu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

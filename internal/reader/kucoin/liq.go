package kucoin

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

	"github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/api"
	"github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/generate/futures/futurespublic"
	"github.com/Kucoin/kucoin-universal-sdk/sdk/golang/pkg/types"
)

// Kucoin_LIQ_Reader streams liquidation executions from KuCoin futures.
type Kucoin_LIQ_Reader struct {
	config        *appconfig.Config
	channels      *liq.Channels
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
	symbols       []string
	symbolSet     map[string]struct{}
	client        api.Client
	ws            futurespublic.FuturesPublicWS
	subscriptions map[string]string
}

// Kucoin_LIQ_NewReader creates a new reader instance.
func Kucoin_LIQ_NewReader(cfg *appconfig.Config, ch *liq.Channels, symbols []string) *Kucoin_LIQ_Reader {
	return &Kucoin_LIQ_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Kucoin_LIQ_Start establishes websocket subscriptions.
func (r *Kucoin_LIQ_Reader) Kucoin_LIQ_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("kucoin liquidation reader already running")
	}
	r.running = true
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.Liquidation
	log := r.log.WithComponent("kucoin_liq_reader").WithFields(logger.Fields{"operation": "Kucoin_LIQ_Start"})

	if !cfg.Enabled {
		log.Warn("kucoin futures liquidation stream disabled via configuration")
		return fmt.Errorf("kucoin futures liquidation stream disabled")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			log.Warn("no symbols configured for kucoin liquidation reader")
			return fmt.Errorf("no symbols configured for kucoin liquidation reader")
		}
		r.symbols = cfg.Symbols
	}

	r.symbolSet = make(map[string]struct{}, len(r.symbols))
	for _, sym := range r.symbols {
		r.symbolSet[strings.ToUpper(sym)] = struct{}{}
	}

	wsOptionBuilder := types.NewWebSocketClientOptionBuilder()
	if cfg.ReadBufferBytes > 0 {
		wsOptionBuilder.WithReadBufferBytes(cfg.ReadBufferBytes)
	}
	if cfg.ReadMessageBuffer > 0 {
		wsOptionBuilder.WithReadMessageBuffer(cfg.ReadMessageBuffer)
	}
	if cfg.WriteMessageBuffer > 0 {
		wsOptionBuilder.WithWriteMessageBuffer(cfg.WriteMessageBuffer)
	}
	wsOptionBuilder.WithEventCallback(func(event types.WebSocketEvent, msg string) {
		if event == types.EventErrorReceived || event == types.EventClientFail {
			log.WithFields(logger.Fields{"event": event.String(), "message": msg}).Warn("kucoin websocket event")
		}
	})

	clientOption := types.NewClientOptionBuilder().
		WithFuturesEndpoint(cfg.URL).
		WithWebSocketClientOption(wsOptionBuilder.Build()).
		Build()

	client := api.NewClient(clientOption)
	ws := client.WsService().NewFuturesPublicWS()
	if ws == nil {
		log.Error("failed to create kucoin futures websocket client")
		return fmt.Errorf("failed to create kucoin futures websocket client")
	}

	if err := ws.Start(); err != nil {
		log.WithError(err).Error("failed to start kucoin websocket service")
		return fmt.Errorf("failed to start kucoin websocket service: %w", err)
	}

	r.client = client
	r.ws = ws
	r.subscriptions = make(map[string]string, len(r.symbols))

	for _, sym := range r.symbols {
		symbol := sym
		id, err := ws.Execution(symbol, func(topic, subject string, data *futurespublic.ExecutionEvent) error {
			return r.handleExecution(topic, subject, data)
		})
		if err != nil {
			log.WithError(err).WithField("symbol", symbol).Error("failed to subscribe to kucoin execution stream")
			continue
		}
		r.subscriptions[strings.ToUpper(symbol)] = id
	}

	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("kucoin liquidation reader started")
	go r.monitorContext()
	return nil
}

// Kucoin_LIQ_Stop cancels subscriptions and shuts down the websocket.
func (r *Kucoin_LIQ_Reader) Kucoin_LIQ_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	ws := r.ws
	subs := r.subscriptions
	r.mu.Unlock()

	r.log.WithComponent("kucoin_liq_reader").Info("stopping kucoin liquidation reader")
	if cancel != nil {
		cancel()
	}
	if ws != nil {
		for _, id := range subs {
			if id != "" {
				ws.UnSubscribe(id)
			}
		}
		ws.Stop()
	}
	r.log.WithComponent("kucoin_liq_reader").Info("kucoin liquidation reader stopped")
}

func (r *Kucoin_LIQ_Reader) monitorContext() {
	<-r.ctx.Done()
	r.Kucoin_LIQ_Stop()
}

func (r *Kucoin_LIQ_Reader) handleExecution(topic, subject string, data *futurespublic.ExecutionEvent) error {
	if data == nil {
		return nil
	}

	if !strings.Contains(strings.ToLower(subject), "liquid") {
		// KuCoin marks liquidation executions via the subject field.
		return nil
	}

	symbol := strings.ToUpper(data.Symbol)
	if len(r.symbolSet) > 0 {
		if _, ok := r.symbolSet[symbol]; !ok {
			return nil
		}
	}

	clone := *data
	clone.CommonResponse = nil

	payload := struct {
		Topic   string                       `json:"topic"`
		Subject string                       `json:"subject"`
		Data    futurespublic.ExecutionEvent `json:"data"`
	}{
		Topic:   topic,
		Subject: subject,
		Data:    clone,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		r.log.WithComponent("kucoin_liq_reader").WithError(err).Warn("failed to marshal kucoin liquidation event")
		return nil
	}

	ts := kucoinTimestampToTime(data.Ts)
	msg := models.RawLiquidationMessage{
		Exchange:  "kucoin",
		Symbol:    symbol,
		Market:    "liquidation",
		Data:      raw,
		Timestamp: ts,
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return nil
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricLiquidationRaw, "kucoin", "liquidation", symbol, "raw")
		r.log.WithComponent("kucoin_liq_reader").WithFields(logger.Fields{"symbol": symbol}).Warn("liquidation raw channel full, dropping message")
	}
	return nil
}

func kucoinTimestampToTime(ts int64) time.Time {
	switch {
	case ts <= 0:
		return time.Now().UTC()
	case ts < 1_000_000_000_000:
		return time.Unix(ts, 0).UTC()
	case ts < 1_000_000_000_000_000:
		return time.UnixMilli(ts).UTC()
	default:
		return time.Unix(0, ts).UTC()
	}
}

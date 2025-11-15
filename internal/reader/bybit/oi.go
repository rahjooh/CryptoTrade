package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	foichannel "cryptoflow/internal/channel/foi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Bybit_FOI_Reader streams open interest updates from Bybit tickers websocket.
type Bybit_FOI_Reader struct {
	config    *appconfig.Config
	channels  *foichannel.Channels
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	running   bool
	log       *logger.Log
	symbols   []string
	symbolSet map[string]struct{}
	category  string
	wsConn    *websocket.Conn
	connMu    sync.Mutex
	wg        sync.WaitGroup
}

// Bybit_FOI_NewReader initialises the reader.
func Bybit_FOI_NewReader(cfg *appconfig.Config, ch *foichannel.Channels, symbols []string, _ string) *Bybit_FOI_Reader {
	return &Bybit_FOI_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Bybit_FOI_Start subscribes to Bybit tickers for configured symbols and emits open interest updates.
func (r *Bybit_FOI_Reader) Bybit_FOI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit open-interest reader already running")
	}
	r.running = true
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.OpenInterest
	log := r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{"operation": "Bybit_FOI_Start"})

	if !cfg.Enabled {
		log.Warn("bybit futures open interest is disabled")
		return fmt.Errorf("bybit futures open interest is disabled")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			log.Warn("no symbols configured for bybit open-interest reader")
			return fmt.Errorf("no symbols configured for bybit open-interest reader")
		}
		r.symbols = cfg.Symbols
	}

	r.category = strings.TrimSpace(cfg.Category)
	if r.category == "" {
		r.category = "linear"
	}

	r.symbolSet = make(map[string]struct{}, len(r.symbols))
	topics := make([]string, 0, len(r.symbols))
	for _, sym := range r.symbols {
		symbol := strings.ToUpper(strings.TrimSpace(sym))
		if symbol == "" {
			continue
		}
		r.symbolSet[symbol] = struct{}{}
		topics = append(topics, fmt.Sprintf("tickers.%s.%s", r.category, symbol))
	}

	if len(topics) == 0 {
		log.Warn("no valid symbols configured for bybit open-interest reader")
		return fmt.Errorf("no valid symbols configured for bybit open-interest reader")
	}

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
		runBybitWebSocket(r.ctx, wsURL, topics, reconnectDelay, log, r.handleTicker, r.trackConn)
	}()

	go r.monitorContext()

	log.WithFields(logger.Fields{"symbols": r.symbols, "category": r.category}).Info("bybit open-interest reader started")
	return nil
}

// Bybit_FOI_Stop terminates the reader.
func (r *Bybit_FOI_Reader) Bybit_FOI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	r.mu.Unlock()

	r.log.WithComponent("bybit_oi_reader").Info("stopping bybit open-interest reader")
	if cancel != nil {
		cancel()
	}
	r.closeActiveConn()
	r.wg.Wait()
	r.log.WithComponent("bybit_oi_reader").Info("bybit open-interest reader stopped")
}

func (r *Bybit_FOI_Reader) monitorContext() {
	<-r.ctx.Done()
	r.Bybit_FOI_Stop()
}

func (r *Bybit_FOI_Reader) handleTicker(raw string) error {
	var ack bybitSubscriptionAck
	if err := json.Unmarshal([]byte(raw), &ack); err == nil && ack.Op != "" {
		if !ack.Success {
			r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{
				"operation": ack.Op,
				"message":   ack.RetMsg,
			}).Warn("open-interest subscription acknowledgement failure")
		}
		return nil
	}

	var payload bybitTickerPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		r.log.WithComponent("bybit_oi_reader").WithError(err).Debug("failed to decode bybit open-interest message")
		return nil
	}

	if !strings.HasPrefix(payload.Topic, "tickers") {
		return nil
	}

	eventTime := time.Now().UTC()
	if payload.Ts > 0 {
		eventTime = time.UnixMilli(payload.Ts).UTC()
	}

	for _, entry := range payload.Data {
		symbol := strings.ToUpper(entry.Symbol)
		if len(r.symbolSet) > 0 {
			if _, ok := r.symbolSet[symbol]; !ok {
				continue
			}
		}

		payloadData := models.BybitFOIResp{
			Symbol:       symbol,
			OpenInterest: entry.OpenInterest,
			Ts:           payload.Ts,
		}

		rawData, err := json.Marshal(payloadData)
		if err != nil {
			r.log.WithComponent("bybit_oi_reader").WithError(err).Warn("failed to marshal bybit open-interest event")
			continue
		}

		msg := models.RawFOIMessage{
			Exchange:  "bybit",
			Symbol:    symbol,
			Market:    "future-openinterest",
			Data:      rawData,
			Timestamp: eventTime,
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return nil
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "bybit", "future-openinterest", symbol, "raw")
			r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{"symbol": symbol}).Warn("open interest raw channel full, dropping message")
		}
	}
	return nil
}

func (r *Bybit_FOI_Reader) trackConn(conn *websocket.Conn) {
	r.connMu.Lock()
	r.wsConn = conn
	r.connMu.Unlock()
}

func (r *Bybit_FOI_Reader) closeActiveConn() {
	r.connMu.Lock()
	conn := r.wsConn
	r.wsConn = nil
	r.connMu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

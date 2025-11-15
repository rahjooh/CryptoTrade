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
	pichannel "cryptoflow/internal/channel/pi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Bybit_PI_Reader streams premium index information from Bybit tickers websocket.
type Bybit_PI_Reader struct {
	config    *appconfig.Config
	channels  *pichannel.Channels
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	running   bool
	log       *logger.Log
	symbols   []string
	category  string
	symbolSet map[string]struct{}
	wsConn    *websocket.Conn
	connMu    sync.Mutex
	wg        sync.WaitGroup
}

// Bybit_PI_NewReader creates a new reader instance.
func Bybit_PI_NewReader(cfg *appconfig.Config, ch *pichannel.Channels, symbols []string) *Bybit_PI_Reader {
	return &Bybit_PI_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Bybit_PI_Start starts the websocket connection and subscriptions.
func (r *Bybit_PI_Reader) Bybit_PI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("bybit premium-index reader already running")
	}
	r.running = true
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.PremiumIndex
	if !cfg.Enabled {
		return fmt.Errorf("bybit premium-index disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for bybit premium-index reader")
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
		symbol := strings.ToUpper(strings.TrimSpace(sym))
		if symbol == "" {
			continue
		}
		r.symbolSet[symbol] = struct{}{}
		args = append(args, fmt.Sprintf("tickers.%s.%s", r.category, symbol))
	}

	if len(args) == 0 {
		return fmt.Errorf("no valid symbols configured for bybit premium-index reader")
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
		runBybitWebSocket(r.ctx, wsURL, args, reconnectDelay, r.log.WithComponent("bybit_pi_reader"), r.handleMessage, r.trackConn)
	}()

	go r.monitorContext()

	r.log.WithComponent("bybit_pi_reader").WithFields(logger.Fields{
		"symbols":  r.symbols,
		"category": r.category,
	}).Info("bybit premium-index reader started")
	return nil
}

// Bybit_PI_Stop disconnects the websocket and cancels workers.
func (r *Bybit_PI_Reader) Bybit_PI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	r.closeActiveConn()
	r.wg.Wait()
	r.log.WithComponent("bybit_pi_reader").Info("bybit premium-index reader stopped")
}

func (r *Bybit_PI_Reader) monitorContext() {
	<-r.ctx.Done()
	r.Bybit_PI_Stop()
}
func (r *Bybit_PI_Reader) handleMessage(raw string) error {
	var ack struct {
		Op      string `json:"op"`
		Success bool   `json:"success"`
		RetMsg  string `json:"ret_msg"`
	}
	if err := json.Unmarshal([]byte(raw), &ack); err == nil && ack.Op != "" {
		if !ack.Success {
			r.log.WithComponent("bybit_pi_reader").WithFields(logger.Fields{
				"op":      ack.Op,
				"message": ack.RetMsg,
			}).Warn("premium-index subscription acknowledgement failure")
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

		mark := parseNumber(entry.MarkPrice)
		index := parseNumber(entry.IndexPrice)
		funding := parseNumber(entry.FundingRate)
		var nextFunding time.Time
		if entry.NextFundingTime != "" {
			if ts, err := strconv.ParseInt(entry.NextFundingTime, 10, 64); err == nil {
				nextFunding = time.UnixMilli(ts).UTC()
			}
		}

		msg := models.RawPIMessage{
			Exchange:        "bybit",
			Symbol:          symbol,
			Market:          "pi",
			MarkPrice:       mark,
			IndexPrice:      index,
			FundingRate:     funding,
			NextFundingTime: nextFunding,
			PremiumIndex:    mark - index,
			Source:          fmt.Sprintf("bybit_%s_ws", r.category),
			Timestamp:       eventTime,
			Payload:         append([]byte(nil), []byte(raw)...),
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, "bybit", "pi", symbol, "raw")
		}
	}
	return nil
}

func (r *Bybit_PI_Reader) trackConn(conn *websocket.Conn) {
	r.connMu.Lock()
	r.wsConn = conn
	r.connMu.Unlock()
}

func (r *Bybit_PI_Reader) closeActiveConn() {
	r.connMu.Lock()
	conn := r.wsConn
	r.wsConn = nil
	r.connMu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

func parseNumber(value string) float64 {
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return parsed
}

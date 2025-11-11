package binance

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

	futures "github.com/adshao/go-binance/v2/futures"
	"github.com/gorilla/websocket"
)

// Binance_OI_Reader streams open-interest updates from Binance futures websockets.
type Binance_OI_Reader struct {
	config   *appconfig.Config
	channels *oichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	localIP  string
}

// Binance_OI_NewReader constructs a new reader for the configured symbols.
func Binance_OI_NewReader(cfg *appconfig.Config, ch *oichannel.Channels, symbols []string, localIP string) *Binance_OI_Reader {
	return &Binance_OI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Binance_OI_Start launches websocket subscriptions per symbol.
func (r *Binance_OI_Reader) Binance_OI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("binance open-interest reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.OpenInterest
	if !cfg.Enabled {
		r.log.WithComponent("binance_oi_reader").Warn("binance open-interest disabled via configuration")
		return fmt.Errorf("binance open-interest disabled")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for binance open-interest reader")
		}
		r.symbols = cfg.Symbols
	}

	for _, symbol := range r.symbols {
		s := strings.ToUpper(symbol)
		r.wg.Add(1)
		go r.streamSymbol(s, cfg)
	}

	r.log.WithComponent("binance_oi_reader").WithFields(logger.Fields{
		"symbols": r.symbols,
	}).Info("binance open-interest reader started")
	return nil
}

// Binance_OI_Stop waits for all websocket workers to exit.
func (r *Binance_OI_Reader) Binance_OI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_oi_reader").Info("stopping binance open-interest reader")
	r.wg.Wait()
	r.log.WithComponent("binance_oi_reader").Info("binance open-interest reader stopped")
}

type binanceOpenInterestEvent struct {
	Event        string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	OpenInterest string `json:"o"`
	HoldValue    string `json:"h"`
}

func (r *Binance_OI_Reader) streamSymbol(symbol string, cfg appconfig.OpenInterestConfig) {
	defer r.wg.Done()

	baseURL := strings.TrimSpace(cfg.URL)
	if baseURL == "" {
		baseURL = futures.BaseWsMainUrl
	}
	baseURL = strings.TrimRight(baseURL, "/")

	reconnect := cfg.ReconnectDelay
	if reconnect <= 0 {
		reconnect = 5 * time.Second
	}

	intervalSuffix := ""
	if cfg.StreamInterval >= time.Second {
		intervalSuffix = fmt.Sprintf("@%ds", int(cfg.StreamInterval/time.Second))
	}

	endpoint := fmt.Sprintf("%s/%s@openInterest%s", baseURL, strings.ToLower(symbol), intervalSuffix)
	intervalLabel := ""
	if cfg.StreamInterval > 0 {
		intervalLabel = cfg.StreamInterval.String()
	}

	dialer := websocket.Dialer{}
	if r.localIP != "" {
		if ip := net.ParseIP(r.localIP); ip != nil {
			dialer.NetDialContext = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}).DialContext
		}
	}

	log := r.log.WithComponent("binance_oi_reader").WithFields(logger.Fields{
		"symbol":   symbol,
		"endpoint": endpoint,
	})

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := dialer.Dial(endpoint, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to binance open-interest stream")
			select {
			case <-time.After(reconnect):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				log.WithError(err).Warn("binance open-interest stream error, reconnecting")
				break
			}
			r.handleMessage(raw, symbol, intervalLabel)
		}

		select {
		case <-time.After(reconnect):
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Binance_OI_Reader) handleMessage(raw []byte, symbol, intervalLabel string) {
	var evt binanceOpenInterestEvent
	if err := json.Unmarshal(raw, &evt); err != nil {
		r.log.WithComponent("binance_oi_reader").WithError(err).Debug("failed to decode open-interest payload")
		return
	}

	value, _ := strconv.ParseFloat(evt.OpenInterest, 64)
	valueUSD, _ := strconv.ParseFloat(evt.HoldValue, 64)
	eventTime := time.UnixMilli(evt.EventTime)
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}

	msg := models.RawOIMessage{
		Exchange:  "binance",
		Symbol:    strings.ToUpper(symbol),
		Market:    "oi",
		Value:     value,
		ValueUSD:  valueUSD,
		Source:    "binance_ws",
		Interval:  intervalLabel,
		Timestamp: eventTime,
		Payload:   append([]byte(nil), raw...),
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricOpenInterestRaw, "binance", "oi", msg.Symbol, "raw")
		r.log.WithComponent("binance_oi_reader").WithFields(logger.Fields{
			"symbol": msg.Symbol,
		}).Warn("dropping binance open-interest message due to backpressure")
	}
}

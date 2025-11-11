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
	pichannel "cryptoflow/internal/channel/pi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Binance_PI_Reader streams premium index (mark price) updates from Binance futures websockets.
type Binance_PI_Reader struct {
	config   *appconfig.Config
	channels *pichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
	localIP  string
}

// Binance_PI_NewReader constructs a premium-index reader instance.
func Binance_PI_NewReader(cfg *appconfig.Config, ch *pichannel.Channels, symbols []string, localIP string) *Binance_PI_Reader {
	return &Binance_PI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Binance_PI_Start launches websocket workers per symbol.
func (r *Binance_PI_Reader) Binance_PI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("binance premium-index reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.PremiumIndex
	if !cfg.Enabled {
		return fmt.Errorf("binance premium-index disabled via configuration")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for binance premium-index reader")
		}
		r.symbols = cfg.Symbols
	}

	for _, sym := range r.symbols {
		symbol := strings.ToUpper(sym)
		r.wg.Add(1)
		go r.streamSymbol(symbol, cfg)
	}

	r.log.WithComponent("binance_pi_reader").WithFields(logger.Fields{
		"symbols": r.symbols,
	}).Info("binance premium-index reader started")
	return nil
}

// Binance_PI_Stop waits for all websocket workers to exit.
func (r *Binance_PI_Reader) Binance_PI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("binance_pi_reader").Info("stopping binance premium-index reader")
	r.wg.Wait()
	r.log.WithComponent("binance_pi_reader").Info("binance premium-index reader stopped")
}

type binanceMarkPricePayload struct {
	Event                string `json:"e"`
	EventTime            int64  `json:"E"`
	Symbol               string `json:"s"`
	MarkPrice            string `json:"p"`
	IndexPrice           string `json:"i"`
	EstimatedSettlePrice string `json:"P"`
	FundingRate          string `json:"r"`
	NextFundingTime      int64  `json:"T"`
}

func (r *Binance_PI_Reader) streamSymbol(symbol string, cfg appconfig.PremiumIndexConfig) {
	defer r.wg.Done()

	baseURL := strings.TrimSpace(cfg.URL)
	if baseURL == "" {
		baseURL = "wss://fstream.binance.com/ws"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	reconnect := cfg.ReconnectDelay
	if reconnect <= 0 {
		reconnect = 5 * time.Second
	}

	intervalSuffix := ""
	switch cfg.StreamInterval {
	case time.Second:
		intervalSuffix = "@1s"
	case 3 * time.Second, 0:
		// default 3s stream, no suffix needed
	default:
		intervalSuffix = "@1s"
	}

	endpoint := fmt.Sprintf("%s/%s@markPrice%s", baseURL, strings.ToLower(symbol), intervalSuffix)

	dialer := websocket.Dialer{}
	if r.localIP != "" {
		if ip := net.ParseIP(r.localIP); ip != nil {
			dialer.NetDialContext = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}).DialContext
		}
	}

	log := r.log.WithComponent("binance_pi_reader").WithFields(logger.Fields{
		"symbol":   symbol,
		"endpoint": endpoint,
	})

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := dialer.Dial(endpoint, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to binance premium-index websocket")
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
				log.WithError(err).Warn("binance premium-index stream error, reconnecting")
				break
			}
			r.handleMessage(symbol, raw)
		}

		select {
		case <-time.After(reconnect):
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Binance_PI_Reader) handleMessage(symbol string, raw []byte) {
	var payload binanceMarkPricePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.log.WithComponent("binance_pi_reader").WithError(err).Debug("failed to decode binance premium-index payload")
		return
	}

	mark := parseFloat(payload.MarkPrice)
	index := parseFloat(payload.IndexPrice)
	estimated := parseFloat(payload.EstimatedSettlePrice)
	funding := parseFloat(payload.FundingRate)

	eventTime := time.UnixMilli(payload.EventTime)
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}

	var nextFunding time.Time
	if payload.NextFundingTime > 0 {
		nextFunding = time.UnixMilli(payload.NextFundingTime).UTC()
	}

	msg := models.RawPIMessage{
		Exchange:             "binance",
		Symbol:               symbol,
		Market:               "pi",
		MarkPrice:            mark,
		IndexPrice:           index,
		EstimatedSettlePrice: estimated,
		FundingRate:          funding,
		NextFundingTime:      nextFunding,
		PremiumIndex:         mark - index,
		Source:               "binance_ws",
		Timestamp:            eventTime.UTC(),
		Payload:              append([]byte(nil), raw...),
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, "binance", "pi", symbol, "raw")
	}
}

func parseFloat(v string) float64 {
	if v == "" {
		return 0
	}
	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return val
}

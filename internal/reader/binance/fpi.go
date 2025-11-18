package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fpichannel "cryptoflow/internal/channel/fpi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Binance_FPI_Reader streams combined mark price updates (premium index) from Binance.
type Binance_FPI_Reader struct {
	config   *appconfig.Config
	channels *fpichannel.Channels
	ctx      context.Context
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	log      *logger.Log

	localIP string
	symbols []string
	running bool
}

// Binance_FPI_NewReader constructs the reader bound to a shard IP.
func Binance_FPI_NewReader(cfg *appconfig.Config, ch *fpichannel.Channels, symbols []string, localIP string) *Binance_FPI_Reader {
	return &Binance_FPI_Reader{
		config:   cfg,
		channels: ch,
		log:      logger.GetLogger(),
		wg:       &sync.WaitGroup{},
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Binance_FPI_Start dials the combined websocket endpoint and begins streaming events.
func (r *Binance_FPI_Reader) Binance_FPI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("binance FPI reader already running")
	}
	r.running = true
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.mu.Unlock()

	cfg := r.config.Source.Binance.Future.FPI
	if !cfg.Enabled {
		return fmt.Errorf("binance FPI disabled via configuration")
	}
	if cfg.Connection != "websocket" {
		return fmt.Errorf("binance FPI expects websocket connection, got %s", cfg.Connection)
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for binance FPI reader")
		}
		r.symbols = cfg.Symbols
	}

	r.wg.Add(1)
	go r.stream(cfg)

	r.log.WithComponent("binance_fpi_reader").WithFields(logger.Fields{
		"symbols": len(r.symbols),
		"ip":      r.localIP,
	}).Info("binance FPI reader started")
	return nil
}

// Binance_FPI_Stop cancels the websocket worker.
func (r *Binance_FPI_Reader) Binance_FPI_Stop() {
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
	r.wg.Wait()
	r.log.WithComponent("binance_fpi_reader").Info("binance FPI reader stopped")
}

type binanceCombinedPayload struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type binanceMarkPriceUpdate struct {
	Event      string `json:"e"`
	EventTime  int64  `json:"E"`
	Symbol     string `json:"s"`
	MarkPrice  string `json:"p"`
	IndexPrice string `json:"i"`
	EstPrice   string `json:"P"`
	Funding    string `json:"r"`
	NextTime   int64  `json:"T"`
}

func (r *Binance_FPI_Reader) stream(cfg appconfig.PremiumIndexConfig) {
	defer r.wg.Done()

	baseURL := strings.TrimSpace(cfg.URL)
	if baseURL == "" {
		baseURL = "wss://fstream.binance.com"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	streams := make([]string, 0, len(r.symbols))
	for _, sym := range r.symbols {
		trim := strings.TrimSpace(sym)
		if trim == "" {
			continue
		}
		streams = append(streams, fmt.Sprintf("%s@markPrice", strings.ToLower(trim)))
	}
	if len(streams) == 0 {
		r.log.WithComponent("binance_fpi_reader").Warn("no valid symbols configured for binance FPI reader")
		return
	}
	endpoint := fmt.Sprintf("%s/stream?streams=%s", baseURL, strings.Join(streams, "/"))

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

	log := r.log.WithComponent("binance_fpi_reader").WithFields(logger.Fields{
		"endpoint": endpoint,
	})

	for {
		if r.ctx.Err() != nil {
			return
		}
		conn, _, err := dialer.DialContext(r.ctx, endpoint, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to binance FPI websocket")
			if !r.waitReconnect(reconnect) {
				continue
			}
			return
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				_ = conn.Close()
				log.WithError(err).Warn("binance FPI websocket error, reconnecting")
				break
			}
			r.handleMessage(raw)
		}

		if !r.waitReconnect(reconnect) {
			continue
		}
		return
	}
}

func (r *Binance_FPI_Reader) waitReconnect(delay time.Duration) bool {
	if delay <= 0 {
		delay = 5 * time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

func (r *Binance_FPI_Reader) handleMessage(raw []byte) {
	var combined binanceCombinedPayload
	payload := binanceMarkPriceUpdate{}

	if err := json.Unmarshal(raw, &combined); err == nil && combined.Stream != "" {
		if err := json.Unmarshal(combined.Data, &payload); err != nil {
			r.log.WithComponent("binance_fpi_reader").WithError(err).Debug("failed to decode combined binance FPI payload")
			return
		}
	} else if err := json.Unmarshal(raw, &payload); err != nil {
		// ignore subscription acknowledgements or malformed events
		return
	}

	if payload.Symbol == "" {
		return
	}

	mark := parseFloat(payload.MarkPrice)
	index := parseFloat(payload.IndexPrice)
	est := parseFloat(payload.EstPrice)
	funding := parseFloat(payload.Funding)

	eventTime := time.UnixMilli(payload.EventTime).UTC()
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}
	var nextTime time.Time
	if payload.NextTime > 0 {
		nextTime = time.UnixMilli(payload.NextTime).UTC()
	}

	msg := models.RawFPI{
		Exchange:             models.ExchangeBinance,
		Market:               "fpi",
		Symbol:               strings.ToUpper(payload.Symbol),
		MarkPrice:            mark,
		IndexPrice:           index,
		EstimatedSettlePrice: est,
		FundingRate:          funding,
		NextFundingTime:      nextTime,
		PremiumIndex:         mark - index,
		EventTime:            eventTime,
		Source:               "binance_stream",
		Payload:              append([]byte(nil), raw...),
	}

	if !r.channels.SendRaw(r.ctx, msg) {
		if r.ctx.Err() != nil {
			return
		}
		metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, models.ExchangeBinance, "fpi", msg.Symbol, "raw")
	}
}

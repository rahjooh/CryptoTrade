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
	fpichannel "cryptoflow/internal/channel/fpi"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	"github.com/gorilla/websocket"
)

// Okx_FPI_Reader streams mark price (premium index) data from OKX websocket.
type Okx_FPI_Reader struct {
	config   *appconfig.Config
	channels *fpichannel.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	log      *logger.Log

	running bool
	symbols []string
	localIP string
	inst    string
}

// Okx_FPI_NewReader returns a configured reader.
func Okx_FPI_NewReader(cfg *appconfig.Config, ch *fpichannel.Channels, symbols []string, localIP string) *Okx_FPI_Reader {
	return &Okx_FPI_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
	}
}

// Okx_FPI_Start connects to the websocket and subscribes to mark-price channels.
func (r *Okx_FPI_Reader) Okx_FPI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("okx FPI reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.FPI
	if !cfg.Enabled {
		return fmt.Errorf("okx FPI disabled via configuration")
	}
	if cfg.Connection != "websocket" {
		return fmt.Errorf("okx FPI expects websocket connection")
	}

	if len(r.symbols) == 0 {
		if len(cfg.Symbols) == 0 {
			return fmt.Errorf("no symbols configured for okx FPI reader")
		}
		r.symbols = cfg.Symbols
	}
	r.inst = strings.TrimSpace(cfg.InstType)
	if r.inst == "" {
		r.inst = "SWAP"
	}

	r.wg.Add(1)
	go r.stream(cfg)

	r.log.WithComponent("okx_fpi_reader").WithFields(logger.Fields{
		"symbols": len(r.symbols),
		"inst":    r.inst,
	}).Info("okx FPI reader started")
	return nil
}

// Okx_FPI_Stop blocks until the worker exits.
func (r *Okx_FPI_Reader) Okx_FPI_Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("okx_fpi_reader").Info("stopping okx FPI reader")
	r.wg.Wait()
	r.log.WithComponent("okx_fpi_reader").Info("okx FPI reader stopped")
}

func (r *Okx_FPI_Reader) stream(cfg appconfig.PremiumIndexConfig) {
	defer r.wg.Done()

	wsURL := cfg.URL
	if wsURL == "" {
		wsURL = "wss://ws.okx.com:8443/ws/v5/public"
	}

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

	log := r.log.WithComponent("okx_fpi_reader")

	for {
		if r.ctx.Err() != nil {
			return
		}

		conn, _, err := dialer.DialContext(r.ctx, wsURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect to okx FPI websocket")
			if r.wait(reconnect) {
				return
			}
			continue
		}

		args := make([]map[string]string, 0, len(r.symbols))
		for _, sym := range r.symbols {
			args = append(args, map[string]string{
				"channel":  "mark-price",
				"instType": r.inst,
				"instId":   sym,
			})
		}
		req := map[string]any{"op": "subscribe", "args": args}
		if err := conn.WriteJSON(req); err != nil {
			conn.Close()
			log.WithError(err).Warn("failed to subscribe to okx mark-price channel")
			if r.wait(reconnect) {
				return
			}
			continue
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				log.WithError(err).Warn("okx FPI websocket error, reconnecting")
				break
			}
			r.handleMessage(raw)
		}

		if r.wait(reconnect) {
			return
		}
	}
}

func (r *Okx_FPI_Reader) wait(delay time.Duration) bool {
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

type okxFpiPayload struct {
	Arg struct {
		Channel  string `json:"channel"`
		InstType string `json:"instType"`
		InstID   string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID     string `json:"instId"`
		MarkPrice  string `json:"markPx"`
		IndexPrice string `json:"idxPx"`
		Timestamp  string `json:"ts"`
	} `json:"data"`
	Event string `json:"event"`
}

func (r *Okx_FPI_Reader) handleMessage(raw []byte) {
	var payload okxFpiPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.log.WithComponent("okx_fpi_reader").WithError(err).Debug("failed to decode okx FPI payload")
		return
	}
	if payload.Event != "" || payload.Arg.Channel != "mark-price" {
		return
	}
	for _, entry := range payload.Data {
		mark := parseFloat(entry.MarkPrice)
		index := parseFloat(entry.IndexPrice)
		ts := parseTimestamp(entry.Timestamp)

		msg := models.RawFPI{
			Exchange:     models.ExchangeOKX,
			Market:       "fpi",
			Symbol:       entry.InstID,
			MarkPrice:    mark,
			IndexPrice:   index,
			PremiumIndex: mark - index,
			EventTime:    ts,
			Source:       "okx_ws",
			Payload:      append([]byte(nil), raw...),
		}

		if !r.channels.SendRaw(r.ctx, msg) {
			if r.ctx.Err() != nil {
				return
			}
			metrics.EmitDropMetric(r.log, metrics.DropMetricPremiumIndexRaw, models.ExchangeOKX, "fpi", entry.InstID, "raw")
		}
	}
}

func parseTimestamp(ts string) time.Time {
	if ts == "" {
		return time.Now().UTC()
	}
	val, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Now().UTC()
	}
	return time.UnixMilli(val).UTC()
}

func parseFloat(v string) float64 {
	if v == "" {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return f
}

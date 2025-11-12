package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appconfig "cryptoflow/config"
	fobd "cryptoflow/internal/channel/fobd"
	metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	"cryptoflow/logger"

	bybit "github.com/bybit-exchange/bybit.go.api"
)

// ---------- Public Reader ----------

// BybitFOBDReader streams futures order book deltas from Bybit and forwards them
// to your Raw channel. It maintains per-symbol sequence state for gap detection.
type Bybit_FOBD_Reader struct {
	// Constructor inputs
	config   *appconfig.Config
	channels *fobd.Channels
	log      *logger.Log
	symbols  []string
	ip       string
	// Lifecycle
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool
}

// Bybit_FOBD_NewReader creates a new delta reader for Bybit futures.
func Bybit_FOBD_NewReader(cfg *appconfig.Config, ch *fobd.Channels, symbols []string, localIP string) *Bybit_FOBD_Reader {
	return &Bybit_FOBD_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		ip:       localIP,
	}
}

// Bybit_FOBD_Start subscribes to order book delta streams for configured symbols.
func (r *Bybit_FOBD_Reader) Bybit_FOBD_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("Bybit_FOBD_Reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.Orderbook.Delta
	log := r.log.WithComponent("bybit_delta_reader").WithFields(logger.Fields{"operation": "Bybit_FOBD_Start"})

	if !cfg.Enabled {
		log.Warn("bybit futures orderbook delta is disabled")
		return fmt.Errorf("bybit futures orderbook delta is disabled")
	}

	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting bybit delta reader")

	r.wg.Add(1)
	go r.stream(r.symbols, cfg.URL)

	log.Info("bybit delta reader started successfully")
	return nil
}

// Bybit_FOBD_Stop terminates all websocket subscriptions.
func (r *Bybit_FOBD_Reader) Bybit_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("bybit_delta_reader").Info("stopping bybit delta reader")
	r.wg.Wait()
	r.log.WithComponent("bybit_delta_reader").Info("bybit delta reader stopped")
}

func (r *Bybit_FOBD_Reader) stream(symbols []string, wsURL string) {
	defer r.wg.Done()

	log := r.log.WithComponent("bybit_delta_reader").WithFields(logger.Fields{
		"symbols": strings.Join(symbols, ","),
		"worker":  "delta_stream",
	})

	var lastMsgMs int64
	updateLast := func() { atomic.StoreInt64(&lastMsgMs, time.Now().UnixMilli()) }

	args := make([]string, len(symbols))
	for i, s := range symbols {
		args[i] = fmt.Sprintf("orderbook.50.%s", s)
	}

	handler := func(message string) error {
		updateLast()
		var base struct {
			Topic string `json:"topic"`
		}
		if err := json.Unmarshal([]byte(message), &base); err != nil {
			return nil
		}
		if base.Topic == "" || !strings.HasPrefix(base.Topic, "orderbook.") {
			return nil
		}
		parts := strings.Split(base.Topic, ".")
		sym := ""
		if len(parts) >= 3 {
			sym = parts[2]
		}

		msg := models.RawFOBDMessage{
			Exchange:  "bybit",
			Symbol:    sym,
			Market:    "future-orderbook-delta",
			Data:      []byte(message),
			Timestamp: time.Now(),
		}

		if r.channels.SendRaw(r.ctx, msg) {
			log.WithFields(logger.Fields{
				"payload_bytes": len(message),
				"topic":         base.Topic,
			}).Debug("delta message forwarded to raw channel")
		} else if r.ctx.Err() != nil {
			return r.ctx.Err()
		} else {
			metrics.EmitDropMetric(r.log, metrics.DropMetricDeltaRaw, "bybit", "future-orderbook-delta", sym, "raw")
			log.Warn("raw delta channel full, dropping message")
		}
		return nil
	}

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if r.ctx.Err() != nil {
			return
		}

		// Reset last message time when (re)connecting
		updateLast()
		closed := make(chan struct{}, 1)
		reconnect := make(chan struct{}, 1)

		ws := bybit.NewBybitPublicWebSocket(wsURL, handler)
		ws.Connect().SendSubscription(args)

		// Heartbeat watchdog: if no messages for > 45s, force reconnect
		watch := time.NewTicker(15 * time.Second)
		go func() {
			for {
				select {
				case <-r.ctx.Done():
					select {
					case reconnect <- struct{}{}:
					default:
					}
					return
				case <-watch.C:
					if time.Since(time.UnixMilli(atomic.LoadInt64(&lastMsgMs))) > 45*time.Second {
						ws.Disconnect()
						select {
						case closed <- struct{}{}:
						default:
						}
						return
					}
				}
			}
		}()

		select {
		case <-r.ctx.Done():
			watch.Stop()
			ws.Disconnect()
			return
		case <-reconnect:
			watch.Stop()
			ws.Disconnect()
		}

		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobd "cryptoflow/internal/channel/fobd"
	"cryptoflow/logger"
	"cryptoflow/models"

	bybit "github.com/bybit-exchange/bybit.go.api"
)

// Bybit_FOBD_Reader streams futures order book deltas from Bybit.
type Bybit_FOBD_Reader struct {
	config   *appconfig.Config
	channels *fobd.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
}

// Bybit_FOBD_NewReader creates a new delta reader for Bybit futures.
func Bybit_FOBD_NewReader(cfg *appconfig.Config, ch *fobd.Channels, symbols []string, localIP string) *Bybit_FOBD_Reader {
	return &Bybit_FOBD_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
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

	var regular []string
	for _, sym := range r.symbols {
		if strings.Contains(sym, "-USDC-SWAP") {
			r.wg.Add(1)
			go r.streamUSDC(sym, cfg.URL)
		} else {
			regular = append(regular, sym)
		}
	}

	if len(regular) > 0 {
		r.wg.Add(1)
		go r.stream(regular, cfg.URL)
	}

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

	args := make([]string, len(symbols))
	for i, s := range symbols {
		args[i] = fmt.Sprintf("orderbook.50.%s", s)
	}

	handler := func(message string) error {
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
			logger.IncrementDeltaRead(len(message))
		} else if r.ctx.Err() != nil {
			return r.ctx.Err()
		} else {
			log.Warn("raw delta channel full, dropping message")
		}
		return nil
	}

	for {
		ws := bybit.NewBybitPublicWebSocket(wsURL, handler)
		ws.Connect().SendSubscription(args)

		select {
		case <-r.ctx.Done():
			ws.Disconnect()
			return
		}
	}

}

func (r *Bybit_FOBD_Reader) streamUSDC(symbol, wsURL string) {
	defer r.wg.Done()

	log := r.log.WithComponent("bybit_delta_reader").WithFields(logger.Fields{
		"symbols": symbol,
		"worker":  "delta_stream_usdc",
	})

	handler := func(message string) error {
		var base struct {
			Topic string `json:"topic"`
		}
		if err := json.Unmarshal([]byte(message), &base); err != nil {
			return nil
		}
		if base.Topic == "" || !strings.HasPrefix(base.Topic, "orderbook.") {
			return nil
		}

		msg := models.RawFOBDMessage{
			Exchange:  "bybit",
			Symbol:    symbol,
			Market:    "future-orderbook-delta",
			Data:      []byte(message),
			Timestamp: time.Now(),
		}

		if r.channels.SendRaw(r.ctx, msg) {
			logger.IncrementDeltaRead(len(message))
		} else if r.ctx.Err() != nil {
			return r.ctx.Err()
		} else {
			log.Warn("raw delta channel full, dropping message")
		}
		return nil
	}

	for {
		ws := bybit.NewBybitPublicWebSocket(wsURL, handler)
		ws.Connect().SendSubscription([]string{fmt.Sprintf("orderbook.50.%s", symbol)})
		select {
		case <-r.ctx.Done():
			ws.Disconnect()
			return
		}
	}
}

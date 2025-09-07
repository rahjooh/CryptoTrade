package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"

	okex "github.com/tfxq/okx-go-sdk"
	wspublic "github.com/tfxq/okx-go-sdk/api/ws"
	publicevt "github.com/tfxq/okx-go-sdk/events/public"
	publicreq "github.com/tfxq/okx-go-sdk/requests/ws/public"
)

// Okx_FOBD_Reader streams futures order book deltas from OKX.
type Okx_FOBD_Reader struct {
	config  *appconfig.Config
	rawChan chan<- models.RawFOBDMessage
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool
	log     *logger.Log
	symbols []string
}

// Okx_FOBD_NewReader creates a new delta reader.
func Okx_FOBD_NewReader(cfg *appconfig.Config, rawChan chan<- models.RawFOBDMessage, symbols []string, localIP string) *Okx_FOBD_Reader {
	return &Okx_FOBD_Reader{
		config:  cfg,
		rawChan: rawChan,
		wg:      &sync.WaitGroup{},
		log:     logger.GetLogger(),
		symbols: symbols,
	}
}

// Okx_FOBD_Start subscribes to order book channels for configured symbols.
func (r *Okx_FOBD_Reader) Okx_FOBD_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("Okx_FOBD_Reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Okx.Future.Orderbook.Delta
	log := r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"operation": "Okx_FOBD_Start"})
	if !cfg.Enabled {
		log.Warn("okx futures orderbook delta is disabled")
		return fmt.Errorf("okx futures orderbook delta is disabled")
	}
	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting okx delta reader")
	r.wg.Add(1)
	go r.stream(r.symbols, cfg.URL)
	log.Info("okx delta reader started successfully")
	return nil
}

// Okx_FOBD_Stop terminates all websocket subscriptions.
func (r *Okx_FOBD_Reader) Okx_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
	r.log.WithComponent("okx_delta_reader").Info("stopping okx delta reader")
	r.wg.Wait()
	r.log.WithComponent("okx_delta_reader").Info("okx delta reader stopped")
}

// stream handles websocket subscription and event forwarding.
func (r *Okx_FOBD_Reader) stream(symbols []string, wsURL string) {
	defer r.wg.Done()
	log := r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"symbols": symbols, "worker": "delta_stream"})

	urlMap := map[bool]okex.BaseURL{false: okex.BaseURL(wsURL)}
	client := wspublic.NewClient(r.ctx, "", "", "", urlMap)
	if err := client.Connect(false); err != nil {
		log.WithError(err).Error("failed to connect websocket")
		return
	}

	ch := make(chan *publicevt.OrderBook)
	for _, sym := range symbols {
		req := publicreq.OrderBook{InstID: sym, Channel: "books-l2-tbt"}
		if err := client.Public.OrderBook(req, ch); err != nil {
			log.WithError(err).Warn("subscription failed")
		}
	}

	for {
		select {
		case <-r.ctx.Done():
			client.Cancel()
			return
		case evt, ok := <-ch:
			if !ok {
				client.Cancel()
				return
			}
			r.handleEvent(evt)
		}
	}
}

// handleEvent converts websocket event to raw delta message.
func (r *Okx_FOBD_Reader) handleEvent(evt *publicevt.OrderBook) {
	if evt == nil || len(evt.Books) == 0 || evt.Arg == nil {
		return
	}
	book := evt.Books[0]
	bids := make([]models.FOBDEntry, len(book.Bids))
	for i, b := range book.Bids {
		bids[i] = models.FOBDEntry{Price: fmt.Sprintf("%f", b.DepthPrice), Quantity: fmt.Sprintf("%f", b.Size)}
	}
	asks := make([]models.FOBDEntry, len(book.Asks))
	for i, a := range book.Asks {
		asks[i] = models.FOBDEntry{Price: fmt.Sprintf("%f", a.DepthPrice), Quantity: fmt.Sprintf("%f", a.Size)}
	}
	inst, _ := evt.Arg.Get("instId")
	symbol, _ := inst.(string)
	resp := models.OkxFOBDResp{
		Symbol:    symbol,
		Action:    evt.Action,
		Timestamp: time.Time(book.TS).UnixMilli(),
		Bids:      bids,
		Asks:      asks,
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Warn("failed to marshal event")
		return
	}
	msg := models.RawFOBDMessage{
		Exchange:  "okx",
		Symbol:    symbol,
		Market:    "future-orderbook-delta",
		Data:      payload,
		Timestamp: time.Now(),
	}
	select {
	case r.rawChan <- msg:
		logger.IncrementDeltaRead(len(payload))
	case <-r.ctx.Done():
	default:
		r.log.WithComponent("okx_delta_reader").Warn("raw delta channel full, dropping message")
	}
}

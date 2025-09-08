package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobd "cryptoflow/internal/channel/fobd"
	"cryptoflow/logger"
	"cryptoflow/models"

	okex "github.com/tfxq/okx-go-sdk"
	okxapi "github.com/tfxq/okx-go-sdk/api"
	publicevt "github.com/tfxq/okx-go-sdk/events/public"
	publicreq "github.com/tfxq/okx-go-sdk/requests/ws/public"
)

// Okx_FOBD_Reader subscribes to OKX websocket streams for swap order book
// deltas and forwards the normalized messages into the raw delta channel.  The
// reader uses okx-go-sdk which manages serialization and connection details.
type Okx_FOBD_Reader struct {
	config   *appconfig.Config
	channels *fobd.Channels
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
	log      *logger.Log
	symbols  []string
}

// Okx_FOBD_NewReader creates a new delta reader.
func Okx_FOBD_NewReader(cfg *appconfig.Config, ch *fobd.Channels, symbols []string, localIP string) *Okx_FOBD_Reader {
	return &Okx_FOBD_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
	}
}

// Okx_FOBD_Start establishes a websocket connection and subscribes to swap
// order book delta streams for all configured symbols.  If the connection drops
// it will be re-established automatically until the context is cancelled.
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
		log.Warn("okx swap orderbook delta is disabled")
		return fmt.Errorf("okx swap orderbook delta is disabled")
	}

	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting okx swap delta reader")
	r.wg.Add(1)
	go r.stream(r.symbols)
	log.Info("okx swap delta reader started successfully")
	return nil
}

// Okx_FOBD_Stop terminates all websocket subscriptions and waits for goroutines
// to finish.
func (r *Okx_FOBD_Reader) Okx_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
	r.log.WithComponent("okx_delta_reader").Info("stopping okx delta reader")
	r.wg.Wait()
	r.log.WithComponent("okx_delta_reader").Info("okx delta reader stopped")
}

// stream handles websocket lifecycle, reconnection and forwarding of events.
func (r *Okx_FOBD_Reader) stream(symbols []string) {
	defer r.wg.Done()
	log := r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"symbols": symbols, "worker": "delta_stream"})

	for {
		if r.ctx.Err() != nil {
			return
		}

		client, err := okxapi.NewClient(r.ctx, "", "", "", okex.NormalServer)
		if err != nil {
			log.WithError(err).Warn("failed to create okx api client, retrying")
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		ch := make(chan *publicevt.OrderBook)
		for _, sym := range symbols {
			req := publicreq.OrderBook{InstID: sym, Channel: "books-l2-tbt"}
			if err := client.Ws.Public.OrderBook(req, ch); err != nil {
				log.WithError(err).Warn("subscription failed")
			}
		}

		for {
			select {
			case <-r.ctx.Done():
				client.Ws.Cancel()
				return
			case <-client.Ws.DoneChan:
				client.Ws.Cancel()
				log.Warn("websocket connection closed, reconnecting")
				goto RECONNECT
			case evt, ok := <-ch:
				if !ok {
					client.Ws.Cancel()
					log.Warn("orderbook channel closed, reconnecting")
					goto RECONNECT
				}
				r.handleEvent(evt)
			}
		}

	RECONNECT:
		time.Sleep(time.Second)
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
		Market:    "swap-orderbook-delta",
		Data:      payload,
		Timestamp: time.Now(),
	}
	if r.channels.SendRaw(r.ctx, msg) {
		logger.IncrementDeltaRead(len(payload))
	} else if r.ctx.Err() != nil {
		return
	} else {
		r.log.WithComponent("okx_delta_reader").Warn("raw delta channel full, dropping message")
	}
}

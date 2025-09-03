package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/internal/symbols"
	"cryptoflow/logger"
	"cryptoflow/models"

	kumex "github.com/Kucoin/kucoin-futures-go-sdk"
	"github.com/gorilla/websocket"
)

// Kucoin_FOBD_Reader streams futures order book deltas from KuCoin.
type Kucoin_FOBD_Reader struct {
	config  *appconfig.Config
	rawChan chan<- models.RawFOBDMessage
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.RWMutex
	running bool
	log     *logger.Log
	symbols []string
	localIP string
}

// Kucoin_FOBD_NewReader creates a new delta reader.
// Symbols defines the markets this reader will subscribe to.
func Kucoin_FOBD_NewReader(cfg *appconfig.Config, rawChan chan<- models.RawFOBDMessage, symbols []string, localIP string) *Kucoin_FOBD_Reader {
	return &Kucoin_FOBD_Reader{
		config:  cfg,
		rawChan: rawChan,
		wg:      &sync.WaitGroup{},
		log:     logger.GetLogger(),
		symbols: symbols,
		localIP: localIP,
	}
}

// Kucoin_FOBD_Start subscribes to level2 streams for configured symbols.
func (r *Kucoin_FOBD_Reader) Kucoin_FOBD_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("delta reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Kucoin.Future.Orderbook.Delta
	log := r.log.WithComponent("kucoin_delta_reader").WithFields(logger.Fields{"operation": "Kucoin_FOBD_Start"})

	if !cfg.Enabled {
		log.Warn("kucoin futures orderbook delta is disabled")
		return fmt.Errorf("kucoin futures orderbook delta is disabled")
	}

	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting delta reader")

	r.wg.Add(1)
	go r.Kucoin_FOBD_stream(r.symbols, cfg.URL)

	log.Info("kucoin delta reader started successfully")
	return nil
}

// Kucoin_FOBD_Stop terminates all websocket subscriptions.
func (r *Kucoin_FOBD_Reader) Kucoin_FOBD_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	r.log.WithComponent("kucoin_delta_reader").Info("stopping delta reader")
	r.wg.Wait()
	r.log.WithComponent("kucoin_delta_reader").Info("delta reader stopped")
}

func (r *Kucoin_FOBD_Reader) Kucoin_FOBD_stream(symbolList []string, wsURL string) {
	defer r.wg.Done()

	baseURL := wsURL
	if parsed, err := url.Parse(wsURL); err == nil {
		baseURL = fmt.Sprintf("https://%s", parsed.Host)
	}

	service := kumex.NewApiService(
		kumex.ApiBaseURIOption(baseURL),
	)

	log := r.log.WithComponent("kucoin_delta_reader").WithFields(logger.Fields{
		"worker": "delta_stream",
	})

	reconnectDelay := 5 * time.Second

	for {
		if r.ctx.Err() != nil {
			return
		}

		if r.localIP != "" {
			if ip := net.ParseIP(r.localIP); ip != nil {
				dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}
				websocket.DefaultDialer.NetDialContext = dialer.DialContext
			}
		}

		rsp, err := service.WebSocketPublicToken()
		if err != nil {
			log.WithError(err).Warn("failed to get websocket token")
			time.Sleep(reconnectDelay)
			continue
		}

		tk := &kumex.WebSocketTokenModel{}
		if err := rsp.ReadData(tk); err != nil {
			log.WithError(err).Warn("failed to read websocket token")
			time.Sleep(reconnectDelay)
			continue
		}

		c := service.NewWebSocketClient(tk)
		mc, ec, err := c.Connect()
		if err != nil {
			log.WithError(err).Warn("failed to connect websocket")
			time.Sleep(reconnectDelay)
			continue
		}

		topics := make([]string, len(symbolList))
		for i, symbol := range symbolList {
			topic := fmt.Sprintf("/contractMarket/level2:%s", symbol)
			topics[i] = topic
			sub := kumex.NewSubscribeMessage(topic, false)
			if err := c.Subscribe(sub); err != nil {
				log.WithFields(logger.Fields{"topic": topic}).WithError(err).Warn("failed to subscribe")
			}
		}

		log.WithFields(logger.Fields{"topics": topics}).Info("subscribed to topics")

		// message loop
		for {
			select {
			case <-r.ctx.Done():
				c.Stop()
				return
			case err := <-ec:
				if err != nil {
					log.WithError(err).Warn("websocket error")
				}
				c.Stop()
				time.Sleep(reconnectDelay)
				goto reconnect
			case msg, ok := <-mc:
				if !ok {
					c.Stop()
					time.Sleep(reconnectDelay)
					goto reconnect
				}
				var data struct {
					Sequence  int64  `json:"sequence"`
					Symbol    string `json:"symbol"`
					Timestamp int64  `json:"timestamp"`
					Changes   struct {
						Bids [][]string `json:"bids"`
						Asks [][]string `json:"asks"`
					} `json:"changes"`
				}
				if err := msg.ReadData(&data); err != nil {
					log.WithError(err).Warn("failed to read level2 data")
					continue
				}

				evt := models.BinanceFOBDResp{
					Symbol:           data.Symbol,
					Time:             data.Timestamp,
					FirstUpdateID:    data.Sequence,
					LastUpdateID:     data.Sequence,
					PrevLastUpdateID: data.Sequence - 1,
				}
				evt.Bids = make([]models.FOBDEntry, len(data.Changes.Bids))
				for i, b := range data.Changes.Bids {
					if len(b) < 2 {
						continue
					}
					evt.Bids[i] = models.FOBDEntry{Price: b[0], Quantity: b[1]}
				}
				evt.Asks = make([]models.FOBDEntry, len(data.Changes.Asks))
				for i, a := range data.Changes.Asks {
					if len(a) < 2 {
						continue
					}
					evt.Asks[i] = models.FOBDEntry{Price: a[0], Quantity: a[1]}
				}

				payload, err := json.Marshal(evt)
				if err != nil {
					log.WithError(err).Warn("failed to marshal event")
					continue
				}

				msgOut := models.RawFOBDMessage{
					Exchange:  "kucoin",
					Symbol:    symbols.ToBinance("kucoin", data.Symbol),
					Market:    "future-orderbook-delta",
					Data:      payload,
					Timestamp: time.Now(),
				}

				select {
				case r.rawChan <- msgOut:
					logger.IncrementDeltaRead(len(payload))
				case <-r.ctx.Done():
					c.Stop()
					return
				default:
					log.Warn("raw delta channel full, dropping message")
				}
			}
		}

	reconnect:
		if r.ctx.Err() != nil {
			return
		}
		log.Warn("kucoin websocket disconnected, reconnecting")
	}
}

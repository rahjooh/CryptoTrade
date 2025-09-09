package okx

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	appconfig "cryptoflow/config"
	fobd "cryptoflow/internal/channel/fobd"
	"cryptoflow/logger"
	"cryptoflow/models"

	"github.com/gorilla/websocket"
)

// Okx_FOBD_Reader subscribes to OKX websocket streams for swap order book
// deltas and forwards the normalized messages into the raw delta channel. The
// reader connects directly to the official OKX websocket without relying on
// third-party SDKs.
type Okx_FOBD_Reader struct {
	config     *appconfig.Config
	channels   *fobd.Channels
	ctx        context.Context
	wg         *sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	log        *logger.Log
	symbols    []string
	localIP    string
	httpClient *http.Client
}

// Okx_FOBD_NewReader creates a new delta reader.
func Okx_FOBD_NewReader(cfg *appconfig.Config, ch *fobd.Channels, symbols []string, localIP string) *Okx_FOBD_Reader {
	return &Okx_FOBD_Reader{
		config:   cfg,
		channels: ch,
		wg:       &sync.WaitGroup{},
		log:      logger.GetLogger(),
		symbols:  symbols,
		localIP:  localIP,
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

	transport := &http.Transport{}
	if r.localIP != "" {
		if ip := net.ParseIP(r.localIP); ip != nil {
			dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}
			transport.DialContext = dialer.DialContext
		}
	}
	r.httpClient = &http.Client{Transport: userAgentTransport{agent: "curl/8.5.0", base: transport}, Timeout: r.config.Reader.Timeout}

	r.symbols = r.validateSymbols(r.symbols)

	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting okx swap delta reader")
	r.wg.Add(1)
	go r.stream(r.symbols, cfg.URL)
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
func (r *Okx_FOBD_Reader) stream(symbols []string, wsURL string) {
	defer r.wg.Done()
	log := r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"symbols": symbols, "worker": "delta_stream"})

	for {
		if r.ctx.Err() != nil {
			return
		}

		dialer := websocket.Dialer{}
		if r.localIP != "" {
			if ip := net.ParseIP(r.localIP); ip != nil {
				dialer.NetDialContext = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}).DialContext
			}
		}

		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.WithError(err).Warn("failed to connect websocket, retrying")
			logger.IncrementRetryCount()
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		args := make([]map[string]string, 0, len(symbols))
		for _, sym := range symbols {
			args = append(args, map[string]string{
				"channel":  "books-l2-tbt",
				"instType": "SWAP",
				"instId":   sym,
			})
		}
		sub := map[string]interface{}{"op": "subscribe", "args": args}
		if err := conn.WriteJSON(sub); err != nil {
			log.WithError(err).Warn("failed to subscribe")
			conn.Close()
			continue
		}

		pingTicker := time.NewTicker(20 * time.Second)
		done := make(chan struct{})
		go func() {
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					conn.WriteMessage(websocket.TextMessage, []byte("{\"op\":\"ping\"}"))
				}
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				close(done)
				conn.Close()
				log.WithError(err).Warn("websocket read error, reconnecting")
				goto RECONNECT
			}
			if !r.processMessage(conn, msg) {
				// non-data message processed
				continue
			}
		}

	RECONNECT:
		time.Sleep(time.Second)
	}
}

func (r *Okx_FOBD_Reader) processMessage(conn *websocket.Conn, msg []byte) bool {
	if data, err := decompress(msg); err == nil {
		msg = data
	}
	// handle ping/pong and subscription events
	var base map[string]json.RawMessage
	if err := json.Unmarshal(msg, &base); err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Debug("failed to decode message")
		return false
	}
	if _, ok := base["event"]; ok {
		var evt struct {
			Event string `json:"event"`
		}
		json.Unmarshal(msg, &evt)
		if evt.Event == "ping" {
			conn.WriteMessage(websocket.TextMessage, []byte("{\"op\":\"pong\"}"))
		}
		return false
	}
	if _, ok := base["ping"]; ok {
		var ping struct {
			Ping int64 `json:"ping"`
		}
		if err := json.Unmarshal(msg, &ping); err == nil {
			resp, _ := json.Marshal(map[string]int64{"pong": ping.Ping})
			conn.WriteMessage(websocket.TextMessage, resp)
		}
		return false
	}

	var evt orderBookEvent
	if err := json.Unmarshal(msg, &evt); err != nil {
		return false
	}
	r.handleEvent(&evt)
	return true
}

func decompress(msg []byte) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(msg))
	defer reader.Close()
	return io.ReadAll(reader)
}

func (r *Okx_FOBD_Reader) validateSymbols(symbols []string) []string {
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", nil)
	if err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Warn("failed to build instruments request")
		return symbols
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Warn("failed to fetch instruments list")
		return symbols
	}
	defer resp.Body.Close()
	var wrapper struct {
		Code string `json:"code"`
		Data []struct {
			InstID string `json:"instId"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		r.log.WithComponent("okx_delta_reader").WithError(err).Warn("failed to decode instruments list")
		return symbols
	}
	valid := make(map[string]struct{}, len(wrapper.Data))
	for _, inst := range wrapper.Data {
		valid[inst.InstID] = struct{}{}
	}
	var filtered []string
	for _, s := range symbols {
		if _, ok := valid[s]; ok {
			filtered = append(filtered, s)
		} else {
			r.log.WithComponent("okx_delta_reader").WithFields(logger.Fields{"symbol": s}).Warn("invalid instrument, skipping")
		}
	}
	return filtered
}

type orderBookEvent struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Action string `json:"action"`
	Data   []struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Ts   string     `json:"ts"`
	} `json:"data"`
}

// handleEvent converts websocket event to raw delta message.
func (r *Okx_FOBD_Reader) handleEvent(evt *orderBookEvent) {
	if evt == nil || len(evt.Data) == 0 {
		return
	}
	book := evt.Data[0]
	bids := make([]models.FOBDEntry, len(book.Bids))
	for i, b := range book.Bids {
		if len(b) >= 2 {
			bids[i] = models.FOBDEntry{Price: b[0], Quantity: b[1]}
		}
	}
	asks := make([]models.FOBDEntry, len(book.Asks))
	for i, a := range book.Asks {
		if len(a) >= 2 {
			asks[i] = models.FOBDEntry{Price: a[0], Quantity: a[1]}
		}
	}
	ts, _ := strconv.ParseInt(book.Ts, 10, 64)
	resp := models.OkxFOBDResp{
		Symbol:    evt.Arg.InstID,
		Action:    evt.Action,
		Timestamp: ts,
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
		Symbol:    evt.Arg.InstID,
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

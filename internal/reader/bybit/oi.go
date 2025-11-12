package bybit

import (
	"context"
	appconfig "cryptoflow/config"
	foi "cryptoflow/internal/channel/oi"
	"cryptoflow/logger"
	"fmt"
	"sync"
)

// ---------- Public Reader (Open Interest) ----------

// Bybit_FOI_Reader streams futures open interest updates from Bybit and forwards them
// to your Raw FOI channel. The topic prefix is configurable, default "tickers".
type Bybit_FOI_Reader struct {
	config   *appconfig.Config
	channels *foi.Channels
	log      *logger.Log
	symbols  []string
	ip       string
	ctx      context.Context
	wg       *sync.WaitGroup
	mu       sync.RWMutex
	running  bool
}

func Bybit_FOI_NewReader(cfg *appconfig.Config, ch *foi.Channels, symbols []string, localIP string) *Bybit_FOI_Reader {
	return &Bybit_FOI_Reader{config: cfg, channels: ch, wg: &sync.WaitGroup{}, log: logger.GetLogger(), symbols: symbols, ip: localIP}
}

func (r *Bybit_FOI_Reader) Bybit_FOI_Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("Bybit_FOI_Reader already running")
	}
	r.running = true
	r.ctx = ctx
	r.mu.Unlock()

	cfg := r.config.Source.Bybit.Future.OpenInterest // expects .Enabled, .URL
	log := r.log.WithComponent("bybit_oi_reader").WithFields(logger.Fields{"operation": "Bybit_FOI_Start"})
	if !cfg.Enabled {
		log.Warn("bybit futures open interest is disabled")
		return fmt.Errorf("bybit futures open interest is disabled")
	}
	log.WithFields(logger.Fields{"symbols": r.symbols}).Info("starting bybit oi reader")

	r.wg.Add(1)
	go r.stream(r.symbols, cfg.URL)
	log.Info("bybit oi reader started successfully")
	return nil
}

func (r *Bybit_FOI_Reader) Bybit_FOI_Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
	r.log.WithComponent("bybit_oi_reader").Info("stopping bybit oi reader")
	r.wg.Wait()
	r.log.WithComponent("bybit_oi_reader").Info("bybit oi reader stopped")
}

func (r *Bybit_FOI_Reader) stream(symbols []string, wsURL string) {
	defer r.wg.Done()
	//topicPrefix = "tickers"
}

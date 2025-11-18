package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	//"github.com/google/uuid"

	appconfig "cryptoflow/config"
	foichannel "cryptoflow/internal/channel/foi"
	//metrics "cryptoflow/internal/metrics"
	"cryptoflow/internal/models"
	//"cryptoflow/internal/symbols"
	"cryptoflow/logger"

	"github.com/sirupsen/logrus"
)

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// FOI WORKER (CORE) ///////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// FOIWorker contains the core logic for transforming raw FOI messages into
// normalized FOI envelopes.
//
// It is intentionally kept small and stateless (no config or channels), so it
// can be reused by multiple FOIProcessor workers. All concurrency, channel
// wiring, and lifecycle management is handled by FOIProcessor; FOIWorker only
// focuses on:
//
//   - reading from foi.raw
//   - decoding exchange-specific FOI payloads
//   - emitting normalized FOI messages into foi.norm
//
// This mirrors the role of `Processor` in your liquidation pipeline.
type FOIWorker struct{}

// NewFOIWorker constructs a new FOIWorker. Currently, no configuration is
// required; this function exists primarily for symmetry and future extension.
func NewFOIWorker() *FOIWorker {
	return &FOIWorker{}
}

// Run is the main FOI processing loop.
//
// It:
//
//  1. Waits for messages on foiRawCh (models.RawFOI).
//  2. Inspects raw.Exchange to determine which exchange-specific flattener
//     to use (binance, bybit, etc.).
//  3. For each raw payload, produces one or more models.NormFOI envelopes.
//  4. Sends normalized messages into foiNormCh.
//  5. Terminates if:
//     - ctx is cancelled, or
//     - foiRawCh is closed.
//
// This follows the same structure as your liquidation `Processor.Run`.
func (w *FOIWorker) Run(
	ctx context.Context,
	foiRawCh <-chan models.RawFOI,
	foiNormCh chan<- models.NormFOI,
) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[foi-processor] context canceled, stopping")
			return

		case raw, ok := <-foiRawCh:
			if !ok {
				log.Printf("[foi-processor] foi.raw closed, stopping")
				return
			}

			switch raw.Exchange {
			case models.ExchangeBinance:
				// Binance FOI: /fapi/v1/openInterest
				env, err := w.flattenBinance(raw)
				if err != nil {
					log.Printf("[foi-processor] binance flatten error: %v", err)
					continue
				}
				select {
				case foiNormCh <- env:
					logFOIEmission(env)
				case <-ctx.Done():
					return
				}

			case models.ExchangeBybit:
				// Bybit FOI: /v5/market/open-interest
				envs, err := w.flattenBybit(raw)
				if err != nil {
					log.Printf("[foi-processor] bybit flatten error: %v", err)
					continue
				}
				for _, env := range envs {
					select {
					case foiNormCh <- env:
						logFOIEmission(env)
					case <-ctx.Done():
						return
					}
				}

			case models.ExchangeOKX:
				envs, err := w.flattenOKX(raw)
				if err != nil {
					log.Printf("[foi-processor] okx flatten error: %v", err)
					continue
				}
				for _, env := range envs {
					select {
					case foiNormCh <- env:
						logFOIEmission(env)
					case <-ctx.Done():
						return
					}
				}

			default:
				// Unknown or unsupported exchange: ignore silently for now.
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////// FOI PIPELINE ORCHESTRATOR /////////////////////////
///////////////////////////////////////////////////////////////////////////////

// FOIProcessor wires FOIWorker to the configured FOI channels, mirroring the
// structure of your LiquidationProcessor:
//
//   - It owns the context, WaitGroup, and running flag.
//   - It spawns N worker goroutines (N = processor.max_workers).
//   - Each worker calls FOIWorker.Run with foi.raw and foi.norm.
//   - Start/Stop follow the same pattern as LiquidationProcessor.
type FOIProcessor struct {
	config   *appconfig.Config
	channels *foichannel.Channels
	worker   *FOIWorker
	ctx      context.Context
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
	log      *logger.Log
}

// NewFOIProcessor constructs a new FOIProcessor that drains the FOI channels.
//
//   - cfg: global app configuration (used mainly for processor.max_workers).
//   - ch:  FOI channels wrapper providing foi.raw and foi.norm.
func NewFOIProcessor(cfg *appconfig.Config, ch *foichannel.Channels) *FOIProcessor {
	return &FOIProcessor{
		config:   cfg,
		channels: ch,
		worker:   NewFOIWorker(),
		log:      logger.GetLogger(),
	}
}

// Start begins draining the raw FOI channel using one or more worker goroutines.
//
// It:
//
//   - Ensures the processor is not already running.
//   - Stores the provided context for cancellation.
//   - Determines worker count from cfg.Processor.MaxWorkers (default = 1).
//   - Spawns worker goroutines, each executing workerLoop.
//
// This mirrors LiquidationProcessor.Start.
func (p *FOIProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("foi processor already running")
	}
	p.running = true
	p.ctx = ctx
	p.mu.Unlock()

	operationLog := p.log.WithComponent("foi_processor").WithFields(logger.Fields{
		"operation": "start",
	})
	operationLog.Info("starting FOI processor")

	workers := 1
	if p.config != nil && p.config.Processor.MaxWorkers > 0 {
		workers = p.config.Processor.MaxWorkers
	}
	operationLog.WithFields(logger.Fields{"workers": workers}).Info("spawning FOI workers")

	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.workerLoop(ctx, i)
	}

	return nil
}

// Stop requests all workers to exit and waits for them to finish.
//
// It:
//
//   - Checks if the processor is currently running.
//   - Clears the running flag.
//   - Waits on the WaitGroup for all worker goroutines to exit.
//
// This mirrors LiquidationProcessor.Stop.
func (p *FOIProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	p.log.WithComponent("foi_processor").Info("stopping FOI processor")
	p.wg.Wait()
	p.log.WithComponent("foi_processor").Info("FOI processor stopped")
}

// workerLoop wraps FOIWorker.Run with logging and channel checks for a single
// worker goroutine.
//
// Each worker:
//
//   - Logs its startup and shutdown.
//   - Verifies channels are configured.
//   - Delegates the actual processing to FOIWorker.Run.
func (p *FOIProcessor) workerLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()

	logEntry := p.log.WithComponent("foi_processor").WithFields(logger.Fields{
		"worker_id": workerID,
		"operation": "worker",
	})
	logEntry.Info("FOI worker started")

	if p.channels == nil {
		logEntry.Warn("FOI channels not configured, worker exiting")
		return
	}

	p.worker.Run(ctx, p.channels.Raw, p.channels.Norm)
	logEntry.Info("FOI worker stopped")
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// EXCHANGE: BINANCE ///////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// flattenBinance transforms a RawFOI message originating from Binance into a
// single models.NormFOI containing a *BinanceNormalizedFOI payload.
//
// Expected raw payload type: models.BinanceFOICurrentResp:
//
//	{
//	  "symbol": "BTCUSDT",
//	  "openInterest": "97880.696",
//	  "time": 1763286138100
//	}
//
// Mapping:
//
//   - symbol         -> BinanceNormalizedFOI.Symbol
//   - openInterest   -> BinanceNormalizedFOI.OpenInterest (float64)
//   - time (ms)      -> BinanceNormalizedFOI.EventTimeMs and NormFOI.Time
//   - ReceivedTimeMs -> current wall-clock at processing time
//
// Behavior:
//
//   - If JSON decoding fails, an error is returned.
//   - If openInterest cannot be parsed, it is set to 0 and no error is returned.
func (w *FOIWorker) flattenBinance(raw models.RawFOI) (models.NormFOI, error) {
	var resp models.BinanceFOICurrentResp
	if err := json.Unmarshal(raw.Payload, &resp); err != nil {
		return models.NormFOI{}, err
	}

	parseF := func(s string) float64 {
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}

	oi := parseF(resp.OpenInterest)
	eventTime := time.UnixMilli(resp.Time).UTC()

	b := &models.BinanceNormFOI{
		Symbol:       resp.Symbol,
		EventTimeMs:  resp.Time,
		OpenInterest: oi,
	}

	return models.NormFOI{
		Exchange: models.ExchangeBinance,
		Time:     eventTime,
		Binance:  b,
	}, nil
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// EXCHANGE: BYBIT /////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// flattenBybit transforms a RawFOI message originating from Bybit into zero,
// one, or many models.NormFOI envelopes containing *BybitNormalizedFOI.
//
// Expected raw payload type: models.BybitFOIOpenInterestResponse:
//
//	GET /v5/market/open-interest
//	  ?category=linear
//	  ?symbol=BTCUSDT
//	  ?intervalTime=5min
//	  ?limit=200
//
// Response (simplified):
//
//	{
//	  "retCode": 0,
//	  "retMsg": "OK",
//	  "result": {
//	    "symbol": "BTCUSDT",
//	    "category": "linear",
//	    "list": [
//	      {
//	        "openInterest": "123456.78900000",
//	        "timestamp": "1669571400000"
//	      }
//	    ],
//	    "nextPageCursor": ""
//	  },
//	  "retExtInfo": {},
//	  "time": 1672053548579
//	}
//
// Each element in result.list becomes one NormFOI:
//
//   - Exchange             = "bybit"
//   - Time                 = event time from list.timestamp
//   - Bybit.Symbol         = result.symbol
//   - Bybit.Category       = result.category
//   - Bybit.Interval       = "5min" (fixed for now)
//   - Bybit.EventTimeMs    = parsed timestamp
//   - Bybit.OpenInterest   = parsed openInterest
//   - Bybit.ReceivedTimeMs = now()
//
// Behavior:
//
//   - If retCode != 0, no rows are emitted (soft failure).
//   - Individual list entries with invalid numeric fields are skipped
//     without failing the entire payload.
func (w *FOIWorker) flattenBybit(raw models.RawFOI) ([]models.NormFOI, error) {
	var resp models.BybitFOIOpenInterestResponse
	if err := json.Unmarshal(raw.Payload, &resp); err != nil {
		return nil, err
	}

	// Non-zero retCode indicates a failure at Bybit level; we do not treat
	// it as an error to the caller but simply emit no normalized rows.
	if resp.RetCode != 0 {
		return nil, nil
	}

	parseF := func(s string) float64 {
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}

	out := make([]models.NormFOI, 0, len(resp.Result.List))

	for _, item := range resp.Result.List {
		if item.OpenInterest == "" || item.Timestamp == "" {
			// Incomplete entry: skip it rather than producing partial rows.
			continue
		}

		oi := parseF(item.OpenInterest)
		if oi == 0 {
			// If parsing failed or returned zero, we still accept it; zero FOI
			// is a valid, albeit uncommon, state. You can tighten this if needed.
		}

		tsMs, err := strconv.ParseInt(item.Timestamp, 10, 64)
		if err != nil {
			// Malformed timestamp; skip just this entry.
			continue
		}
		eventTime := time.UnixMilli(tsMs).UTC()

		b := &models.BybitNormalizedFOI{
			Symbol:       resp.Result.Symbol,
			Category:     resp.Result.Category,
			Interval:     "5min", // matches requested intervalTime
			EventTimeMs:  tsMs,
			OpenInterest: oi,
		}

		env := models.NormFOI{
			Exchange: models.ExchangeBybit,
			Time:     eventTime,
			Bybit:    b,
		}
		out = append(out, env)
	}

	return out, nil
}

// ---------------- OKX ----------------

// flattenOKX converts an OKX FOI websocket payload into one or more
// models.NormFOI entries, each embedding an *OKXNormalizedFOI.
//
// Expected payload:
//
//	{
//	  "arg": {
//	    "channel": "open-interest",
//	    "instId": "BTC-USDT-SWAP"
//	  },
//	  "data": [
//	    {
//	      "instId": "BTC-USDT-SWAP",
//	      "instType": "SWAP",
//	      "oi": "2216113.01000000309",
//	      "oiCcy": "22161.1301000000309",
//	      "oiUsd": "1939251795.54769270396321",
//	      "ts": "1743041250440"
//	    }
//	  ]
//	}
//
// Each entry in data[] becomes one NormFOI with:
//
//   - Exchange            = "okx"
//   - Time                = event time derived from ts (ms)
//   - OKX.InstID          = instId
//   - OKX.InstType        = instType
//   - OKX.OI              = parsed oi
//   - OKX.OICcy           = parsed oiCcy
//   - OKX.OIUsd           = parsed oiUsd
//   - OKX.EventTimeMs     = parsed ts
//   - OKX.ReceivedTimeMs  = now()
func (w *FOIWorker) flattenOKX(raw models.RawFOI) ([]models.NormFOI, error) {
	var evt models.OKXFOIEvent
	if err := json.Unmarshal(raw.Payload, &evt); err != nil {
		return nil, err
	}

	parseF := func(s string) float64 {
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}

	out := make([]models.NormFOI, 0, len(evt.Data))

	for _, d := range evt.Data {
		tsMs, err := strconv.ParseInt(d.Ts, 10, 64)
		if err != nil {
			// If timestamp is invalid, skip this single entry.
			continue
		}
		eventTime := time.UnixMilli(tsMs).UTC()

		okxNorm := &models.OKXNormalizedFOI{
			InstID:      d.InstID,
			InstType:    d.InstType,
			OI:          parseF(d.OI),
			OICcy:       parseF(d.OICcy),
			OIUsd:       parseF(d.OIUsd),
			EventTimeMs: tsMs,
		}

		env := models.NormFOI{
			Exchange: models.ExchangeOKX,
			Time:     eventTime,
			OKX:      okxNorm,
		}
		out = append(out, env)
	}

	return out, nil
}

func logFOIEmission(env models.NormFOI) {
	log := logger.GetLogger()
	if log == nil || !log.IsLevelEnabled(logrus.DebugLevel) {
		return
	}

	fields := logger.Fields{
		"exchange": env.Exchange,
	}
	if !env.Time.IsZero() {
		fields["timestamp"] = env.Time
	}
	switch env.Exchange {
	case models.ExchangeBinance:
		if env.Binance != nil {
			fields["symbol"] = env.Binance.Symbol
			fields["open_interest"] = env.Binance.OpenInterest
		}
	case models.ExchangeBybit:
		if env.Bybit != nil {
			fields["symbol"] = env.Bybit.Symbol
			fields["category"] = env.Bybit.Category
			fields["interval"] = env.Bybit.Interval
			fields["open_interest"] = env.Bybit.OpenInterest
		}
	case models.ExchangeOKX:
		if env.OKX != nil {
			fields["inst_id"] = env.OKX.InstID
			fields["inst_type"] = env.OKX.InstType
			fields["oi"] = env.OKX.OI
		}
	}
	log.WithComponent("foi_worker").WithFields(fields).Debug("emitted normalized FOI envelope")
}

package dashboard

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"cryptoflow/config"
	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

//go:embed templates/*.tmpl assets/*
var embeddedFS embed.FS

// Server hosts the Gin-powered monitoring dashboard for CryptoFlow.
type Server struct {
	cfg               config.DashboardConfig
	log               *logger.Log
	metricStore       *metricStore
	logStore          *logStore
	metricHandler     metrics.MetricHandlerID
	metricObserver    metrics.MetricHandlerID
	httpServer        *http.Server
	refreshIntervalMs int
	resourceSampler   *resourceSampler
	exchanges         []ExchangeMetadata
	marketIndex       map[string]marketIndexEntry
	dropLog           *dropLog
	mirror            *s3Mirror
}

type marketIndexEntry struct {
	exchange *ExchangeMetadata
	market   *MarketMetadata
}

type channelSample struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type channelSeries struct {
	Samples  []channelSample `json:"samples"`
	Capacity float64         `json:"capacity,omitempty"`
}

// NewServer constructs a dashboard server when the dashboard feature is enabled.
// When the dashboard is disabled the returned server will be nil.
func NewServer(appCfg *config.Config, log *logger.Log) (*Server, error) {
	if appCfg == nil {
		return nil, fmt.Errorf("dashboard requires application configuration")
	}

	cfg := appCfg.Dashboard
	if !cfg.Enabled {
		return nil, nil
	}

	cfg.Address = normalizeAddress(cfg.Address)

	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 5 * time.Second
	}

	if cfg.LogHistory <= 0 {
		cfg.LogHistory = 200
	}

	if cfg.MetricsHistory <= 0 {
		cfg.MetricsHistory = 200
	}

	metricStore := newMetricStore(cfg.MetricsHistory)
	handlerID := metrics.RegisterMetricHandler(metricStore.handle)

	logStore := newLogStore(cfg.LogHistory)
	log.AddHook(logStore)

	sampler := newResourceSampler(cfg.MetricsHistory, cfg.RefreshInterval, "/", log)

	dropLog := newDropLog(cfg.LogHistory * 2)
	exchanges := buildExchangeMetadata(appCfg)
	marketIndex := buildMarketIndex(exchanges)

	server := &Server{
		cfg:               cfg,
		log:               log,
		metricStore:       metricStore,
		logStore:          logStore,
		metricHandler:     handlerID,
		refreshIntervalMs: int(cfg.RefreshInterval / time.Millisecond),
		resourceSampler:   sampler,
		exchanges:         exchanges,
		marketIndex:       marketIndex,
		dropLog:           dropLog,
	}

	if server.refreshIntervalMs <= 0 {
		server.refreshIntervalMs = int((5 * time.Second) / time.Millisecond)
	}

	server.metricObserver = metrics.RegisterMetricHandler(server.observeMetric)

	if cfg.Mirror.Enabled {
		mirror, err := newS3Mirror(server, cfg.Mirror, appCfg.Storage.S3, log)
		if err != nil {
			log.WithComponent("dashboard").WithError(err).Warn("failed to initialise dashboard mirror")
		} else {
			server.mirror = mirror
		}
	}

	return server, nil
}

func buildMarketIndex(exchanges []ExchangeMetadata) map[string]marketIndexEntry {
	if len(exchanges) == 0 {
		return nil
	}
	index := make(map[string]marketIndexEntry)
	for i := range exchanges {
		exchange := &exchanges[i]
		for j := range exchange.Markets {
			market := &exchange.Markets[j]
			if market.Key == "" {
				continue
			}
			key := marketLookupKey(exchange.Name, market.Key)
			index[key] = marketIndexEntry{
				exchange: exchange,
				market:   market,
			}
		}
	}
	return index
}

// Run starts the dashboard HTTP server and blocks until the provided context is
// cancelled or the underlying HTTP server exits with an error.
func (s *Server) Run(ctx context.Context, appName string) error {
	if s == nil {
		return nil
	}

	defer s.cleanup()

	router, err := s.buildRouter(appName)
	if err != nil {
		return err
	}

	if s.resourceSampler != nil {
		s.resourceSampler.start(ctx)
	}
	if s.mirror != nil {
		s.mirror.start(ctx)
	}

	s.httpServer = &http.Server{
		Addr:    s.cfg.Address,
		Handler: router,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		<-errCh
		return nil
	case err := <-errCh:
		if err == nil {
			return nil
		}
		return err
	}
}

func (s *Server) cleanup() {
	metrics.UnregisterMetricHandler(s.metricHandler)
	metrics.UnregisterMetricHandler(s.metricObserver)
	if s.logStore != nil {
		s.logStore.close()
	}
	if s.resourceSampler != nil {
		s.resourceSampler.stop()
	}
	if s.mirror != nil {
		s.mirror.stop()
	}
}

// Address reports the network address the dashboard server listens on.
func (s *Server) Address() string {
	if s == nil {
		return ""
	}
	return s.cfg.Address
}

func (s *Server) buildRouter(appName string) (*gin.Engine, error) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	// Allow running behind load balancers and accessing the dashboard from
	// public networks by trusting all proxies by default. Users can
	// override Gin's trusted proxy list via the GIN_TRUSTED_PROXIES
	// environment variable if needed.
	if err := router.SetTrustedProxies(nil); err != nil {
		return nil, err
	}

	tmpl := template.Must(template.New("dashboard").ParseFS(embeddedFS, "templates/index.tmpl"))
	router.SetHTMLTemplate(tmpl)

	if assetsFS, err := fsSub("assets"); err == nil {
		router.StaticFS("/assets", http.FS(assetsFS))
	}

	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{
			"AppName":           appName,
			"RefreshIntervalMs": s.refreshIntervalMs,
		})
	})

	router.GET("/api/metrics", func(c *gin.Context) {
		metricsSnapshot := s.metricStore.snapshot()
		payload := make([]gin.H, 0, len(metricsSnapshot))
		for _, m := range metricsSnapshot {
			payload = append(payload, gin.H{
				"timestamp": m.Timestamp.Format(time.RFC3339Nano),
				"component": m.Component,
				"name":      m.Name,
				"value":     m.Value,
				"type":      m.Type,
				"fields":    m.Fields,
			})
		}
		c.JSON(http.StatusOK, gin.H{"metrics": payload})
	})

	router.GET("/api/logs", func(c *gin.Context) {
		logsSnapshot := s.logStore.snapshot()
		payload := make([]gin.H, 0, len(logsSnapshot))
		for _, l := range logsSnapshot {
			payload = append(payload, gin.H{
				"timestamp": l.Timestamp.Format(time.RFC3339Nano),
				"level":     l.Level,
				"component": l.Component,
				"message":   l.Message,
				"fields":    l.Fields,
			})
		}
		c.JSON(http.StatusOK, gin.H{"logs": payload})
	})

	router.GET("/api/resources", func(c *gin.Context) {
		snapshots := s.resourceSampler.snapshot()
		payload := make([]gin.H, 0, len(snapshots))
		for _, snap := range snapshots {
			payload = append(payload, gin.H{
				"timestamp":      snap.Timestamp.Format(time.RFC3339Nano),
				"cpu_percent":    snap.CPUPercent,
				"memory_used":    snap.MemoryUsed,
				"memory_total":   snap.MemoryTotal,
				"memory_percent": snap.MemoryPct,
				"disk_used":      snap.DiskUsed,
				"disk_total":     snap.DiskTotal,
				"disk_percent":   snap.DiskPct,
			})
		}
		c.JSON(http.StatusOK, gin.H{"resources": payload})
	})

	router.GET("/api/exchanges", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"exchanges": s.exchanges})
	})

	router.GET("/api/exchanges/:exchange/:market", func(c *gin.Context) {
		s.handleMarketDetail(c)
	})

	return router, nil
}

func (s *Server) handleMarketDetail(c *gin.Context) {
	if s == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "dashboard server unavailable"})
		return
	}
	exchangeParam := strings.ToLower(strings.TrimSpace(c.Param("exchange")))
	marketParam := strings.ToLower(strings.TrimSpace(c.Param("market")))
	if exchangeParam == "" || marketParam == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "exchange and market required"})
		return
	}

	entry, ok := s.marketIndex[marketLookupKey(exchangeParam, marketParam)]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown exchange or market"})
		return
	}

	marketData := *entry.market
	response := gin.H{
		"exchange": gin.H{
			"name":         entry.exchange.Name,
			"display_name": entry.exchange.DisplayName,
		},
		"market":       marketData,
		"channels":     s.channelSeries(marketData.Channels),
		"drops":        s.aggregateDropMetrics(entry.exchange.Name, marketData.Key),
		"logs":         s.filterLogs(entry.exchange.Name, marketData.Key),
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
	}

	c.JSON(http.StatusOK, response)
}

func fsSub(path string) (fs.FS, error) {
	sub, err := fs.Sub(embeddedFS, path)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func normalizeAddress(addr string) string {
	addr = strings.TrimSpace(addr)

	if addr == "" {
		return "0.0.0.0:8080"
	}

	if strings.Contains(addr, "://") {
		if parsed, err := url.Parse(addr); err == nil {
			if host := parsed.Host; host != "" {
				addr = host
			} else if parsed.Opaque != "" {
				addr = parsed.Opaque
			}
		}
	}

	if strings.HasPrefix(addr, ":") {
		if len(addr) > 1 && addr[1] >= '0' && addr[1] <= '9' {
			return "0.0.0.0" + addr
		}
	}

	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		if host == "" || host == "*" {
			host = "0.0.0.0"
		}
		if port == "" {
			port = "8080"
		}
		return net.JoinHostPort(host, port)
	}

	if ip := net.ParseIP(addr); ip != nil {
		return net.JoinHostPort(addr, "8080")
	}

	if !strings.Contains(addr, ":") {
		return net.JoinHostPort(addr, "8080")
	}

	return addr
}

func marketLookupKey(exchange, market string) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	market = strings.ToLower(strings.TrimSpace(market))
	return exchange + "|" + market
}

func (s *Server) observeMetric(metric metrics.Metric) {
	if s == nil || s.dropLog == nil {
		return
	}
	s.dropLog.add(metric)
}

func (s *Server) channelSeries(metricNames []string) map[string]channelSeries {
	if s == nil || len(metricNames) == 0 {
		return nil
	}
	desired := make(map[string]struct{}, len(metricNames))
	for _, name := range metricNames {
		if name == "" {
			continue
		}
		desired[name] = struct{}{}
	}
	if len(desired) == 0 {
		return nil
	}

	snapshot := s.metricStore.snapshot()
	result := make(map[string]channelSeries, len(desired))
	for _, metric := range snapshot {
		if _, ok := desired[metric.Name]; !ok {
			continue
		}
		value, ok := toFloat(metric.Value)
		if !ok {
			continue
		}
		ts := metric.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		series := result[metric.Name]
		series.Samples = append(series.Samples, channelSample{
			Timestamp: ts,
			Value:     value,
		})
		if capacity, ok := extractCapacity(metric.Fields); ok {
			series.Capacity = capacity
		}
		result[metric.Name] = series
	}

	for name, series := range result {
		if len(series.Samples) > 150 {
			series.Samples = append([]channelSample(nil), series.Samples[len(series.Samples)-150:]...)
			result[name] = series
		}
	}

	return result
}

func (s *Server) aggregateDropMetrics(exchange, market string) map[string]int64 {
	if s == nil {
		return nil
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	market = strings.ToLower(strings.TrimSpace(market))
	if exchange == "" {
		return nil
	}

	snapshot := s.metricStore.snapshot()
	aggregates := make(map[string]int64)
	for _, metric := range snapshot {
		if _, ok := dropMetricNames[metric.Name]; !ok {
			continue
		}
		metricExchange := strings.ToLower(fieldString(metric.Fields, "exchange"))
		if metricExchange != exchange {
			continue
		}
		metricMarket := strings.ToLower(fieldString(metric.Fields, "market"))
		if market != "" && metricMarket != market {
			continue
		}
		value, ok := toFloat(metric.Value)
		if !ok {
			continue
		}
		aggregates[metric.Name] += int64(value)
	}
	return aggregates
}

func (s *Server) filterLogs(exchange, market string) []logRecord {
	if s == nil {
		return nil
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	market = strings.ToLower(strings.TrimSpace(market))
	if exchange == "" {
		return nil
	}
	logs := s.logStore.snapshot()
	filtered := make([]logRecord, 0, len(logs))
	for _, entry := range logs {
		if matchesLog(entry, exchange, market) {
			filtered = append(filtered, entry)
		}
	}
	if limit := s.cfg.LogHistory; limit > 0 && len(filtered) > limit {
		filtered = append([]logRecord(nil), filtered[len(filtered)-limit:]...)
	}
	return filtered
}

func matchesLog(entry logRecord, exchange, market string) bool {
	entryExchange := strings.ToLower(fieldString(entry.Fields, "exchange"))
	entryMarket := strings.ToLower(fieldString(entry.Fields, "market"))
	if entryExchange == exchange {
		if market == "" || entryMarket == market {
			return true
		}
	}
	if entry.Component != "" && strings.Contains(strings.ToLower(entry.Component), exchange) {
		if market == "" || entryMarket == market {
			return true
		}
	}
	return false
}

func extractCapacity(fields map[string]interface{}) (float64, bool) {
	if len(fields) == 0 {
		return 0, false
	}
	value, ok := fields["capacity"]
	if !ok {
		return 0, false
	}
	capacity, ok := toFloat(value)
	if !ok || capacity <= 0 {
		return 0, false
	}
	return capacity, true
}

func toFloat(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		if v == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func (s *Server) snapshotState() mirrorSnapshot {
	if s == nil {
		return mirrorSnapshot{}
	}
	metricsSnapshot := s.metricStore.snapshot()
	logsSnapshot := s.logStore.snapshot()
	resourceSnapshot := s.resourceSampler.snapshot()
	var drops []dropEntry
	if s.dropLog != nil {
		drops = s.dropLog.snapshot()
	}

	exchanges := make([]ExchangeMetadata, len(s.exchanges))
	copy(exchanges, s.exchanges)

	return mirrorSnapshot{
		GeneratedAt: time.Now().UTC(),
		Exchanges:   exchanges,
		Metrics:     metricsSnapshot,
		Logs:        logsSnapshot,
		Resources:   resourceSnapshot,
		Drops:       drops,
	}
}

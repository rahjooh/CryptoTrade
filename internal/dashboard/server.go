package dashboard

import (
	"context"
	"embed"
	"errors"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	"net/url"
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
	httpServer        *http.Server
	refreshIntervalMs int
	resourceSampler   *resourceSampler
}

// NewServer constructs a dashboard server when the dashboard feature is enabled.
// When the dashboard is disabled the returned server will be nil.
func NewServer(cfg config.DashboardConfig, log *logger.Log) (*Server, error) {
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

	server := &Server{
		cfg:               cfg,
		log:               log,
		metricStore:       metricStore,
		logStore:          logStore,
		metricHandler:     handlerID,
		refreshIntervalMs: int(cfg.RefreshInterval / time.Millisecond),
		resourceSampler:   sampler,
	}

	if server.refreshIntervalMs <= 0 {
		server.refreshIntervalMs = int((5 * time.Second) / time.Millisecond)
	}

	return server, nil
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
	if s.logStore != nil {
		s.logStore.close()
	}
	if s.resourceSampler != nil {
		s.resourceSampler.stop()
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

	return router, nil
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

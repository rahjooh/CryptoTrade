package dashboard

import (
	"context"
	"embed"
	"errors"
	"html/template"
	"io/fs"
	"net/http"
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
}

// NewServer constructs a dashboard server when the dashboard feature is enabled.
// When the dashboard is disabled the returned server will be nil.
func NewServer(cfg config.DashboardConfig, log *logger.Log) (*Server, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if cfg.Address == "" {
		cfg.Address = ":8080"
	}

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

	server := &Server{
		cfg:               cfg,
		log:               log,
		metricStore:       metricStore,
		logStore:          logStore,
		metricHandler:     handlerID,
		refreshIntervalMs: int(cfg.RefreshInterval / time.Millisecond),
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

	return router, nil
}

func fsSub(path string) (fs.FS, error) {
	sub, err := fs.Sub(embeddedFS, path)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

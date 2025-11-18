package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"cryptoflow/config"
	"cryptoflow/internal/metrics"
	"cryptoflow/logger"
)

type s3Mirror struct {
	cfg    config.DashboardMirrorConfig
	s3cfg  config.S3Config
	client *s3.Client
	log    *logger.Log
	server *Server

	ticker *time.Ticker
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type mirrorSnapshot struct {
	GeneratedAt time.Time          `json:"generated_at"`
	Exchanges   []ExchangeMetadata `json:"exchanges"`
	Metrics     []metrics.Metric   `json:"metrics"`
	Logs        []logRecord        `json:"logs"`
	Resources   []resourceSnapshot `json:"resources"`
	Drops       []dropEntry        `json:"drops"`
}

func newS3Mirror(server *Server, cfg config.DashboardMirrorConfig, s3cfg config.S3Config, log *logger.Log) (*s3Mirror, error) {
	if server == nil {
		return nil, fmt.Errorf("dashboard server is not initialised")
	}
	if !cfg.Enabled {
		return nil, nil
	}
	if !s3cfg.Enabled {
		return nil, fmt.Errorf("storage.s3 is disabled")
	}
	if s3cfg.Bucket == "" {
		return nil, fmt.Errorf("storage.s3.bucket is required for dashboard mirror")
	}
	if s3cfg.Region == "" {
		return nil, fmt.Errorf("storage.s3.region is required for dashboard mirror")
	}

	if cfg.Interval <= 0 {
		cfg.Interval = 4 * time.Minute
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "dashboard"
	}
	if cfg.DropLogName == "" {
		cfg.DropLogName = "dropped-messages.log"
	}

	ctx := context.Background()
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(s3cfg.Region),
	}
	if s3cfg.AccessKeyID != "" && s3cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				s3cfg.AccessKeyID,
				s3cfg.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if s3cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(s3cfg.Endpoint)
		}
		o.UsePathStyle = s3cfg.PathStyle
	})

	return &s3Mirror{
		cfg:    cfg,
		s3cfg:  s3cfg,
		client: client,
		log:    log,
		server: server,
	}, nil
}

func (m *s3Mirror) start(ctx context.Context) {
	if m == nil {
		return
	}
	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.ticker = time.NewTicker(m.cfg.Interval)
	m.log.WithComponent("dashboard_mirror").WithFields(logger.Fields{
		"bucket":   m.s3cfg.Bucket,
		"prefix":   m.cfg.Prefix,
		"interval": m.cfg.Interval.String(),
	}).Info("starting dashboard mirror")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.flush(childCtx, "startup")
		for {
			select {
			case <-childCtx.Done():
				return
			case <-m.ticker.C:
				m.flush(childCtx, "interval")
			}
		}
	}()
}

func (m *s3Mirror) stop() {
	if m == nil {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.wg.Wait()
	m.log.WithComponent("dashboard_mirror").Info("dashboard mirror stopped")
}

func (m *s3Mirror) flush(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	snapshot := m.server.snapshotState()

	stateData, err := json.Marshal(snapshot)
	if err != nil {
		m.log.WithComponent("dashboard_mirror").WithError(err).Warn("failed to serialize dashboard snapshot")
		return
	}

	if err := m.putObject(ctx, m.objectKey("state.json"), stateData, "application/json"); err != nil {
		m.log.WithComponent("dashboard_mirror").WithError(err).Warn("failed to upload dashboard snapshot")
		return
	}

	if err := m.writeLogFiles(ctx, snapshot.Logs); err != nil {
		m.log.WithComponent("dashboard_mirror").WithError(err).Warn("failed to upload log files")
		return
	}

	if err := m.writeDropLog(ctx, snapshot.Drops); err != nil {
		m.log.WithComponent("dashboard_mirror").WithError(err).Warn("failed to upload drop log")
		return
	}

	m.log.WithComponent("dashboard_mirror").WithFields(logger.Fields{
		"reason":  reason,
		"metrics": len(snapshot.Metrics),
		"logs":    len(snapshot.Logs),
		"drops":   len(snapshot.Drops),
	}).Debug("dashboard snapshot mirrored to S3")
}

func (m *s3Mirror) writeDropLog(ctx context.Context, entries []dropEntry) error {
	var buf bytes.Buffer
	for _, entry := range entries {
		line, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to serialize drop entry: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return m.putObject(ctx, m.objectKey(m.cfg.DropLogName), buf.Bytes(), "text/plain")
}

func (m *s3Mirror) writeLogFiles(ctx context.Context, logs []logRecord) error {
	if len(logs) == 0 {
		return nil
	}
	byLevel := map[string][]logRecord{}
	for _, entry := range logs {
		level := strings.ToLower(entry.Level)
		switch level {
		case "warning", "error", "fatal", "panic":
			byLevel[level] = append(byLevel[level], entry)
		}
	}
	if len(byLevel) == 0 {
		return nil
	}
	for level, entries := range byLevel {
		key := path.Join(m.cfg.Prefix, "logs", fmt.Sprintf("%s-%s.log", level, time.Now().UTC().Format("20060102T150405Z")))
		if err := m.writeLogEntries(ctx, key, entries); err != nil {
			return err
		}
	}
	return nil
}

func (m *s3Mirror) writeLogEntries(ctx context.Context, key string, entries []logRecord) error {
	if len(entries) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, entry := range entries {
		line, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to serialize log record: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return m.putObject(ctx, key, buf.Bytes(), "text/plain")
}

func (m *s3Mirror) putObject(ctx context.Context, key string, data []byte, contentType string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := m.client.PutObject(uploadCtx, &s3.PutObjectInput{
		Bucket:      aws.String(m.s3cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

func (m *s3Mirror) objectKey(name string) string {
	prefix := strings.Trim(m.cfg.Prefix, "/")
	if prefix == "" {
		return name
	}
	return path.Join(prefix, name)
}

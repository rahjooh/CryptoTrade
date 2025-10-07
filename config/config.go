package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	Cryptoflow        CryptoflowConfig        `yaml:"cryptoflow"`
	Metrics           MetricsConfig           `yaml:"metrics"`
	Channels          ChannelsConfig          `yaml:"channels"`
	Reader            ReaderConfig            `yaml:"reader"`
	Processor         ProcessorConfig         `yaml:"processor"`
	Writer            WriterConfig            `yaml:"writer"`
	Source            SourceConfig            `yaml:"source"`
	Storage           StorageConfig           `yaml:"storage"`
	Logging           LoggingConfig           `yaml:"logging"`
	ExchangeRateLimit ExchangeRateLimitConfig `yaml:"exchange_rate_limit"`
}

type CryptoflowConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
}

type MetricsConfig struct {
	UsedWeight  bool `yaml:"used_weight"`
	ChannelSize bool `yaml:"channel_size"`
}

type ChannelsConfig struct {
	RawBuffer       int `yaml:"raw_buffer"`
	ProcessedBuffer int `yaml:"processed_buffer"`
	ErrorBuffer     int `yaml:"error_buffer"`
	PoolSize        int `yaml:"pool_size"`
}

type ReaderConfig struct {
	MaxWorkers     int                  `yaml:"max_workers"`
	Timeout        time.Duration        `yaml:"timeout"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	RateLimit      RateLimitConfig      `yaml:"rate_limit"`
	Retry          RetryConfig          `yaml:"retry"`
	Validation     ValidationConfig     `yaml:"validation"`
}

type ProcessorConfig struct {
	MaxWorkers   int           `yaml:"max_workers"`
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}

type CircuitBreakerConfig struct {
	FailureThreshold    int           `yaml:"failure_threshold"`
	RecoveryTimeout     time.Duration `yaml:"recovery_timeout"`
	HalfOpenMaxRequests int           `yaml:"half_open_max_requests"`
}

type RateLimitConfig struct {
	RequestsPerSecond int `yaml:"requests_per_second"`
	BurstSize         int `yaml:"burst_size"`
}

type ExchangeRateLimitConfig struct {
	Binance ExchangeRateLimit `yaml:"binance"`
	Bybit   ExchangeRateLimit `yaml:"bybit"`
	Kucoin  ExchangeRateLimit `yaml:"kucoin"`
	Okx     ExchangeRateLimit `yaml:"okx"`
}

type ExchangeRateLimit struct {
	RequestWeight int64 `yaml:"request_weight"`
	Orders        int64 `yaml:"orders"`
}

type RetryConfig struct {
	MaxAttempts       int           `yaml:"max_attempts"`
	BaseDelay         time.Duration `yaml:"base_delay"`
	MaxDelay          time.Duration `yaml:"max_delay"`
	BackoffMultiplier int           `yaml:"backoff_multiplier"`
}

type ValidationConfig struct {
	EnablePriceValidation    bool    `yaml:"enable_price_validation"`
	EnableQuantityValidation bool    `yaml:"enable_quantity_validation"`
	MaxSpreadPercentage      float64 `yaml:"max_spread_percentage"`
}

type WriterConfig struct {
	MaxWorkers   int                `yaml:"max_workers"`
	Batch        BatchConfig        `yaml:"batch"`
	Buffer       BufferConfig       `yaml:"buffer"`
	Partitioning PartitioningConfig `yaml:"partitioning"`
	Formats      FormatsConfig      `yaml:"formats"`
	Compression  string             `yaml:"compression"`
}

type BatchConfig struct {
	Size      int           `yaml:"size"`
	Timeout   time.Duration `yaml:"timeout"`
	MaxMemory string        `yaml:"max_memory"`
}

type BufferConfig struct {
	MaxSize               int           `yaml:"max_size"`
	SnapshotFlushInterval time.Duration `yaml:"snapshot_flush_interval"`
	DeltaFlushInterval    time.Duration `yaml:"delta_flush_interval"`
	MemoryThreshold       float64       `yaml:"memory_threshold"`
}

type PartitioningConfig struct {
	Scheme         string   `yaml:"scheme"`
	TimeFormat     string   `yaml:"time_format"`
	AdditionalKeys []string `yaml:"additional_keys"`
}

type FormatsConfig struct {
	Parquet ParquetConfig `yaml:"parquet"`
	Avro    AvroConfig    `yaml:"avro"`
}

type ParquetConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Compression string `yaml:"compression"`
	PageSize    int    `yaml:"page_size"`
}

type AvroConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Compression string `yaml:"compression"`
}

type ConnectionPoolConfig struct {
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	MaxConnsPerHost int           `yaml:"max_conns_per_host"`
	IdleConnTimeout time.Duration `yaml:"idle_conn_timeout"`
}

type SourceConfig struct {
	Binance BinanceSourceConfig `yaml:"binance"`
	Bybit   BybitSourceConfig   `yaml:"bybit"`
	Kucoin  KucoinSourceConfig  `yaml:"kucoin"`
	Okx     OkxSourceConfig     `yaml:"okx"`
}

type BinanceSourceConfig struct {
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	Future         BinanceFutureConfig  `yaml:"future"`
}

type BybitSourceConfig struct {
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	Future         BybitFutureConfig    `yaml:"future"`
}

type KucoinSourceConfig struct {
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	Future         KucoinFutureConfig   `yaml:"future"`
}

type OkxSourceConfig struct {
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	Future         OkxFutureConfig      `yaml:"future"`
}

type BinanceFutureConfig struct {
	Orderbook BinanceFutureOrderbookConfig `yaml:"orderbook"`
}

type BybitFutureConfig struct {
	Orderbook BybitFutureOrderbookConfig `yaml:"orderbook"`
}

type KucoinFutureConfig struct {
	Orderbook KucoinFutureOrderbookConfig `yaml:"orderbook"`
}

type OkxFutureConfig struct {
	Orderbook OkxFutureOrderbookConfig `yaml:"orderbook"`
}

type BinanceFutureOrderbookConfig struct {
	Snapshots BinanceSnapshotConfig `yaml:"snapshots"`
	Delta     BinanceDeltaConfig    `yaml:"delta"`
}

type BybitFutureOrderbookConfig struct {
	Snapshots BybitSnapshotConfig `yaml:"snapshots"`
	Delta     BybitDeltaConfig    `yaml:"delta"`
}

type KucoinFutureOrderbookConfig struct {
	Snapshots KucoinSnapshotConfig `yaml:"snapshots"`
	Delta     KucoinDeltaConfig    `yaml:"delta"`
}

type OkxFutureOrderbookConfig struct {
	Snapshots OkxSnapshotConfig `yaml:"snapshots"`
	Delta     OkxDeltaConfig    `yaml:"delta"`
}

type BinanceSnapshotConfig struct {
	Enabled           bool     `yaml:"enabled"`
	Connection        string   `yaml:"connection"`
	URL               string   `yaml:"url"`
	Limit             int      `yaml:"limit"`
	IntervalMs        int      `yaml:"interval_ms"`
	Symbols           []string `yaml:"symbols"`
	ConcurrentSymbols int      `yaml:"concurrent_symbols"`
	BatchSize         int      `yaml:"batch_size"`
}

type BybitSnapshotConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Connection string   `yaml:"connection"`
	URL        string   `yaml:"url"`
	Limit      int      `yaml:"limit"`
	IntervalMs int      `yaml:"interval_ms"`
	Symbols    []string `yaml:"symbols"`
}

type KucoinSnapshotConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Connection string   `yaml:"connection"`
	URL        string   `yaml:"url"`
	Limit      int      `yaml:"limit"`
	IntervalMs int      `yaml:"interval_ms"`
	Symbols    []string `yaml:"symbols"`
}

type OkxSnapshotConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Connection string   `yaml:"connection"`
	URL        string   `yaml:"url"`
	Limit      int      `yaml:"limit"`
	IntervalMs int      `yaml:"interval_ms"`
	Symbols    []string `yaml:"symbols"`
}

type BinanceDeltaConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Connection string   `yaml:"connection"`
	URL        string   `yaml:"url"`
	IntervalMs int      `yaml:"interval_ms"`
	Symbols    []string `yaml:"symbols"`
}

type BybitDeltaConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Connection string   `yaml:"connection"`
	URL        string   `yaml:"url"`
	IntervalMs int      `yaml:"interval_ms"`
	Symbols    []string `yaml:"symbols"`
}

type KucoinDeltaConfig struct {
	Enabled           bool     `yaml:"enabled"`
	Connection        string   `yaml:"connection"`
	URL               string   `yaml:"url"`
	IntervalMs        int      `yaml:"interval_ms"`
	Symbols           []string `yaml:"symbols"`
	ReadBufferBytes   int      `yaml:"read_buffer_bytes"`
	ReadMessageBuffer int      `yaml:"read_message_buffer"`
}

type OkxDeltaConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Connection string   `yaml:"connection"`
	URL        string   `yaml:"url"`
	IntervalMs int      `yaml:"interval_ms"`
	Symbols    []string `yaml:"symbols"`
}

type StorageConfig struct {
	S3  S3Config  `yaml:"s3"`
	GCS GCSConfig `yaml:"gcs"`
}

type S3Config struct {
	Enabled           bool   `yaml:"enabled"`
	Bucket            string `yaml:"bucket"`
	Region            string `yaml:"region"`
	Endpoint          string `yaml:"endpoint"`
	PathStyle         bool   `yaml:"path_style"`
	UploadConcurrency int    `yaml:"upload_concurrency"`
	PartSize          string `yaml:"part_size"`
	AccessKeyID       string `yaml:"access_key_id"`
	SecretAccessKey   string `yaml:"secret_access_key"`
}

type GCSConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Bucket    string `yaml:"bucket"`
	ProjectID string `yaml:"project_id"`
}

type LoggingConfig struct {
	Level         string                 `yaml:"level"`
	Format        string                 `yaml:"format"`
	Output        string                 `yaml:"output"`
	MaxAge        int                    `yaml:"max_age"`
	Fields        map[string]interface{} `yaml:"fields"`
	DashboardName string                 `yaml:"dashboard_name"`
}

func LoadConfig(path string) (*Config, error) {
	// Read configuration file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := Config{
		Metrics: MetricsConfig{
			UsedWeight:  true,
			ChannelSize: true,
		},
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override S3 settings from environment variables if available
	if config.Storage.S3.Enabled {
		if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
			config.Storage.S3.AccessKeyID = strings.TrimSpace(v)
		}
		if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
			config.Storage.S3.SecretAccessKey = strings.TrimSpace(v)
		}
		if v := os.Getenv("AWS_REGION"); v != "" {
			config.Storage.S3.Region = strings.TrimSpace(v)
		}
		if v := os.Getenv("S3_BUCKET"); v != "" {
			config.Storage.S3.Bucket = strings.TrimSpace(v)
		}
	}

	// Validate configuration
	config.Storage.S3.Bucket = strings.TrimSpace(config.Storage.S3.Bucket)

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &config, nil
}

func validateConfig(cfg *Config) error {
	if cfg.Cryptoflow.Name == "" {
		return fmt.Errorf("cryptoflow.name is required")
	}

	if cfg.Cryptoflow.Version == "" {
		return fmt.Errorf("cryptoflow.version is required")
	}

	if cfg.Channels.RawBuffer <= 0 {
		return fmt.Errorf("channels.raw_buffer must be greater than 0")
	}

	if cfg.Reader.MaxWorkers <= 0 {
		return fmt.Errorf("reader.max_workers must be greater than 0")
	}

	if cfg.Processor.MaxWorkers <= 0 {
		return fmt.Errorf("processor.max_workers must be greater than 0")
	}
	if cfg.Processor.BatchSize <= 0 {
		return fmt.Errorf("processor.batch_size must be greater than 0")
	}
	if cfg.Processor.BatchTimeout <= 0 {
		return fmt.Errorf("processor.batch_timeout must be greater than 0")
	}

	if cfg.Writer.Buffer.SnapshotFlushInterval <= 0 {
		return fmt.Errorf("writer.buffer.snapshot_flush_interval must be greater than 0")
	}
	if cfg.Writer.Buffer.DeltaFlushInterval <= 0 {
		return fmt.Errorf("writer.buffer.delta_flush_interval must be greater than 0")
	}

	if cfg.Storage.S3.Enabled {
		if cfg.Storage.S3.Bucket == "" {
			return fmt.Errorf("storage.s3.bucket is required when S3 is enabled")
		}
		if cfg.Storage.S3.Region == "" {
			return fmt.Errorf("storage.s3.region is required when S3 is enabled")
		}
		if cfg.Storage.S3.AccessKeyID == "" || cfg.Storage.S3.SecretAccessKey == "" {
			return fmt.Errorf("storage.s3.access_key_id and storage.s3.secret_access_key are required when S3 is enabled")
		}
		if !isValidS3Bucket(cfg.Storage.S3.Bucket) {
			return fmt.Errorf("storage.s3.bucket '%s' is invalid", cfg.Storage.S3.Bucket)
		}
	}

	return nil
}

var s3BucketRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func isValidS3Bucket(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if strings.Contains(name, "..") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return false
	}
	return s3BucketRegexp.MatchString(name)
}

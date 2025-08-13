package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	//"cryptoflow/logger"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Cryptoflow CryptoflowConfig `yaml:"cryptoflow"`
	Channels   ChannelsConfig   `yaml:"channels"`
	Reader     ReaderConfig     `yaml:"reader"`
	Processor  ProcessorConfig  `yaml:"processor"`
	Writer     WriterConfig     `yaml:"writer"`
	Source     SourceConfig     `yaml:"source"`
	Storage    StorageConfig    `yaml:"storage"`
	Monitoring MonitoringConfig `yaml:"monitoring"`
	Logging    LoggingConfig    `yaml:"logging"`
}

type CryptoflowConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
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
	MaxSize         int           `yaml:"max_size"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	MemoryThreshold float64       `yaml:"memory_threshold"`
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
}

type BinanceSourceConfig struct {
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	Future         BinanceFutureConfig  `yaml:"future"`
}

type BinanceFutureConfig struct {
	Orderbook BinanceFutureOrderbookConfig `yaml:"orderbook"`
}

type BinanceFutureOrderbookConfig struct {
	Snapshots BinanceSnapshotConfig `yaml:"snapshots"`
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

type StorageConfig struct {
	S3  S3Config  `yaml:"s3"`
	GCS GCSConfig `yaml:"gcs"`
}

type S3Config struct {
	Enabled           bool          `yaml:"enabled"`
	Region            string        `yaml:"region"`
	Endpoint          string        `yaml:"endpoint"`
	PathStyle         bool          `yaml:"path_style"`
	UploadConcurrency int           `yaml:"upload_concurrency"`
	PartSize          string        `yaml:"part_size"`
	AccessKeyID       string        `yaml:"access_key_id"`
	SecretAccessKey   string        `yaml:"secret_access_key"`
	TableARN          string        `yaml:"table_arn"`
	FlushInterval     time.Duration `yaml:"flush_interval"`
}

type GCSConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Bucket    string `yaml:"bucket"`
	ProjectID string `yaml:"project_id"`
}

type MonitoringConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Health  HealthConfig  `yaml:"health"`
	Alerts  AlertsConfig  `yaml:"alerts"`
}

type MetricsConfig struct {
	Enabled            bool          `yaml:"enabled"`
	Port               int           `yaml:"port"`
	Path               string        `yaml:"path"`
	CollectionInterval time.Duration `yaml:"collection_interval"`
}

type HealthConfig struct {
	Port          int           `yaml:"port"`
	Path          string        `yaml:"path"`
	CheckInterval time.Duration `yaml:"check_interval"`
}

type AlertsConfig struct {
	Enabled    bool                   `yaml:"enabled"`
	WebhookURL string                 `yaml:"webhook_url"`
	Thresholds AlertsThresholdsConfig `yaml:"thresholds"`
}

type AlertsThresholdsConfig struct {
	ErrorRate   float64 `yaml:"error_rate"`
	BufferUsage float64 `yaml:"buffer_usage"`
	LatencyP99  int     `yaml:"latency_p99"`
}

type LoggingConfig struct {
	Level  string                 `yaml:"level"`
	Format string                 `yaml:"format"`
	Output string                 `yaml:"output"`
	Fields map[string]interface{} `yaml:"fields"`
}

func LoadConfig(path string) (*Config, error) {
	// Read configuration file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
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
		if v := os.Getenv("S3_TABLE_ARN"); v != "" {
			config.Storage.S3.TableARN = strings.TrimSpace(v)
		}
	}

	// Validate configuration
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

	if cfg.Storage.S3.Enabled {
		if cfg.Storage.S3.Region == "" {
			return fmt.Errorf("storage.s3.region is required when S3 is enabled")
		}
		if cfg.Storage.S3.AccessKeyID == "" || cfg.Storage.S3.SecretAccessKey == "" {
			return fmt.Errorf("storage.s3.access_key_id and storage.s3.secret_access_key are required when S3 is enabled")
		}
		if cfg.Storage.S3.TableARN == "" {
			return fmt.Errorf("storage.s3.table_arn is required when S3 is enabled (set storage.s3.table_arn or S3_TABLE_ARN env var)")
		}
		if cfg.Storage.S3.FlushInterval <= 0 {
			return fmt.Errorf("storage.s3.flush_interval must be greater than 0 when S3 is enabled")
		}
	}

	return nil
}

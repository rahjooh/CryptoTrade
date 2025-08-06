package config

import (
	"fmt"
	"os"
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
	Exchanges  ExchangesConfig  `yaml:"exchanges"`
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
	WriteBuffer     int `yaml:"write_buffer"`
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

type ExchangesConfig struct {
	Binance  ExchangeConfig `yaml:"binance"`
	Coinbase ExchangeConfig `yaml:"coinbase"`
}

type ExchangeConfig struct {
	Enabled        bool                 `yaml:"enabled"`
	BaseURL        string               `yaml:"base_url"`
	Endpoints      []EndpointConfig     `yaml:"endpoints"`
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
}

type EndpointConfig struct {
	Name       string        `yaml:"name"`
	Path       string        `yaml:"path"`
	Limit      int           `yaml:"limit"`
	IntervalMs int           `yaml:"interval_ms"`
	Symbols    []string      `yaml:"symbols"`
	Timeout    time.Duration `yaml:"timeout"`
}

type ConnectionPoolConfig struct {
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	MaxConnsPerHost int           `yaml:"max_conns_per_host"`
	IdleConnTimeout time.Duration `yaml:"idle_conn_timeout"`
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
	//log := logger.GetLogger().WithComponent("config")

	//log.WithFields(logger.Fields{"path": path}).Info("loading configuration file")

	data, err := os.ReadFile(path)
	if err != nil {
		//log.WithError(err).WithFields(logger.Fields{"path": path}).Error("failed to read config file")
		fmt.Print("failed to read config file")
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		//log.WithError(err).Error("failed to parse config file")
		fmt.Print("ffailed to parse config file")
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		//log.WithError(err).Error("configuration validation failed")
		fmt.Print("configuration validation failed")
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	fmt.Print("failed to read config file")

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
		if cfg.Storage.S3.Bucket == "" {
			return fmt.Errorf("storage.s3.bucket is required when S3 is enabled")
		}
		if cfg.Storage.S3.Region == "" {
			return fmt.Errorf("storage.s3.region is required when S3 is enabled")
		}
	}

	return nil
}

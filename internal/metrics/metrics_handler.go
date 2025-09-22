package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"cryptoflow/config"
	"cryptoflow/logger"
)

type Feature string

const (
	FeatureUsedWeight  Feature = "used_weight"
	FeatureChannelSize Feature = "channel_size"
)

// Metric represents a structured metric event emitted within the application.
type Metric struct {
	Timestamp time.Time
	Component string
	Name      string
	Value     interface{}
	Type      string
	Fields    logger.Fields
}

// MetricHandler consumes structured metric events for downstream processing.
type MetricHandler func(Metric)

// MetricHandlerID uniquely identifies a registered metric handler.
type MetricHandlerID uint64

var (
	defaultMetricsConfig = config.MetricsConfig{
		UsedWeight:  true,
		ChannelSize: true,
	}
	metricsConfig atomic.Pointer[config.MetricsConfig]

	metricHandlersMu    sync.RWMutex
	metricHandlers      = make(map[MetricHandlerID]MetricHandler)
	nextMetricHandlerID MetricHandlerID
)

func init() {
	cfg := defaultMetricsConfig
	metricsConfig.Store(&cfg)
}

// Configure updates the metrics feature switches used by the metrics package.
func Configure(cfg config.MetricsConfig) {
	copyCfg := cfg
	metricsConfig.Store(&copyCfg)
}

// IsFeatureEnabled reports whether the provided feature flag is enabled.
func IsFeatureEnabled(feature Feature) bool {
	if feature == "" {
		return true
	}
	cfg := metricsConfig.Load()
	if cfg == nil {
		return true
	}
	switch feature {
	case FeatureUsedWeight:
		return cfg.UsedWeight
	case FeatureChannelSize:
		return cfg.ChannelSize
	default:
		return true
	}
}

func metricFeature(name string) Feature {
	switch name {
	case "used_weight":
		return FeatureUsedWeight
	case "fobs_raw_buffer_length", "fobs_norm_buffer_length", "fobd_raw_buffer_length", "fobd_norm_buffer_length":
		return FeatureChannelSize
	default:
		return ""
	}
}

func isMetricEnabled(name string) bool {
	return IsFeatureEnabled(metricFeature(name))
}

// RegisterMetricHandler registers a handler that will receive every emitted metric.
// A zero identifier is returned when the provided handler is nil.
func RegisterMetricHandler(handler MetricHandler) MetricHandlerID {
	if handler == nil {
		return 0
	}

	metricHandlersMu.Lock()
	defer metricHandlersMu.Unlock()

	nextMetricHandlerID++
	id := nextMetricHandlerID
	metricHandlers[id] = handler
	return id
}

// UnregisterMetricHandler removes the handler associated with the given identifier.
func UnregisterMetricHandler(id MetricHandlerID) {
	if id == 0 {
		return
	}

	metricHandlersMu.Lock()
	delete(metricHandlers, id)
	metricHandlersMu.Unlock()
}

func recordMetric(log *logger.Log, component, name string, value interface{}, metricType string, fields logger.Fields) (Metric, bool) {
	if name == "" {
		return Metric{}, false
	}

	if metricType == "" {
		metricType = "counter"
	}

	userFields := cloneFields(fields)

	if log == nil {
		log = logger.GetLogger()
	}

	logFields := make(logger.Fields, len(userFields)+3)
	for k, v := range userFields {
		logFields[k] = v
	}
	logFields["metric"] = name
	logFields["metric_type"] = metricType
	logFields["value"] = value

	log.WithComponent(component).WithFields(logFields).Info("metric")

	metric := Metric{
		Timestamp: time.Now(),
		Component: component,
		Name:      name,
		Value:     value,
		Type:      metricType,
		Fields:    userFields,
	}

	dispatchMetric(metric)
	return metric, true
}

func dispatchMetric(metric Metric) {
	metricHandlersMu.RLock()
	if len(metricHandlers) == 0 {
		metricHandlersMu.RUnlock()
		return
	}

	handlers := make([]MetricHandler, 0, len(metricHandlers))
	for _, handler := range metricHandlers {
		if handler != nil {
			handlers = append(handlers, handler)
		}
	}
	metricHandlersMu.RUnlock()

	for _, handler := range handlers {
		handler(metric)
	}
}

func cloneFields(fields logger.Fields) logger.Fields {
	if len(fields) == 0 {
		return logger.Fields{}
	}

	copied := make(logger.Fields, len(fields))
	for k, v := range fields {
		copied[k] = v
	}
	return copied
}

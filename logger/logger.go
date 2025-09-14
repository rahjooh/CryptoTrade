package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// Fields type alias for logrus.Fields to maintain compatibility
type Fields map[string]interface{}

// Log wraps logrus.Logger with additional functionality
type Log struct {
	*logrus.Logger
}

// Entry wraps logrus.Entry with additional functionality
type Entry struct {
	*logrus.Entry
}

var globalLogger *Log

func init() {
	globalLogger = Logger()
}

func Logger() *Log {
	logger := logrus.New()
	logger.SetReportCaller(true)

	// Determine log level from environment variable
	levelStr := os.Getenv("LOG_LEVEL")
	if levelStr == "" {
		levelStr = "info"
	}
	switch strings.ToLower(levelStr) {
	case "report":
		logger.SetLevel(logrus.InfoLevel)
	default:
		if lvl, err := logrus.ParseLevel(strings.ToLower(levelStr)); err == nil {
			logger.SetLevel(lvl)
		} else {
			logger.SetLevel(logrus.InfoLevel)
		}
	}

	callerPrettyfier := func(f *runtime.Frame) (string, string) {
		file := filepath.Base(f.File)
		return "", fmt.Sprintf("%s:%d", file, f.Line)
	}

	// Set default formatter
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
		CallerPrettyfier: callerPrettyfier,
	})
	logger.AddHook(&callerHook{})
	return &Log{Logger: logger}
}

func GetLogger() *Log {
	return globalLogger
}

func (l *Log) WithComponent(component string) *Entry {
	return &Entry{Entry: l.Logger.WithField("component", component)}
}

func (l *Log) WithFields(fields Fields) *Entry {
	return &Entry{Entry: l.Logger.WithFields(logrus.Fields(fields))}
}

func (l *Log) WithError(err error) *Entry {
	return &Entry{Entry: l.Logger.WithError(err)}
}

// WithEnv attaches environment variable values to the log entry
func (l *Log) WithEnv(envs ...string) *Entry {
	fields := logrus.Fields{}
	for _, env := range envs {
		fields[env] = os.Getenv(env)
	}
	return &Entry{Entry: l.Logger.WithFields(fields)}
}

func (e *Entry) WithComponent(component string) *Entry {
	return &Entry{Entry: e.Entry.WithField("component", component)}
}

func (e *Entry) WithFields(fields Fields) *Entry {
	return &Entry{Entry: e.Entry.WithFields(logrus.Fields(fields))}
}

func (e *Entry) WithError(err error) *Entry {
	return &Entry{Entry: e.Entry.WithError(err)}
}

// WithEnv attaches environment variable values to the log entry
func (e *Entry) WithEnv(envs ...string) *Entry {
	fields := logrus.Fields{}
	for _, env := range envs {
		fields[env] = os.Getenv(env)
	}
	return &Entry{Entry: e.Entry.WithFields(fields)}
}

// Convert Entry methods to return our Entry type
func (e *Entry) Info(args ...interface{}) {
	e.Entry.Info(args...)
}

func (e *Entry) Warn(args ...interface{}) {
	if component, ok := e.Entry.Data["component"].(string); ok {
		recordWarn(component)
	}
	e.Entry.Warn(args...)
}

func (e *Entry) Debug(args ...interface{}) {
	e.Entry.Debug(args...)
}

func (e *Entry) Error(args ...interface{}) {
	if component, ok := e.Entry.Data["component"].(string); ok {
		recordError(component)
	}
	e.Entry.Error(args...)
}

// LogMetric method for Entry
func (e *Entry) LogMetric(component string, metric string, value interface{}, metricType string, fields Fields) {
	if fields == nil {
		fields = make(Fields)
	}
	if metricType == "" {
		metricType = "counter"
	}
	fields["metric"] = metric
	fields["value"] = value
	fields["metric_type"] = metricType

	e.WithComponent(component).WithFields(fields).Info("metric")

	// attempt to publish metric to CloudWatch
	var val float64
	switch v := value.(type) {
	case int:
		val = float64(v)
	case int32:
		val = float64(v)
	case int64:
		val = float64(v)
	case float32:
		val = float64(v)
	case float64:
		val = v
	default:
		return
	}

	dims := []cwtypes.Dimension{{Name: aws.String("component"), Value: aws.String(component)}}
	for k, v := range fields {
		if k == "metric" || k == "metric_type" || k == "value" {
			continue
		}
		if s, ok := v.(string); ok {
			dims = append(dims, cwtypes.Dimension{Name: aws.String(k), Value: aws.String(s)})
		}
	}

	data := []cwtypes.MetricDatum{{
		MetricName: aws.String(metric),
		Dimensions: dims,
		Unit:       cwtypes.StandardUnitCount,
		Value:      aws.Float64(val),
	}}
	publishMetrics(context.Background(), data)
}

// Configure sets up the logger with the provided configuration
func (l *Log) Configure(level string, format string, output string, maxAge int) error {
	if env := os.Getenv("LOG_LEVEL"); env != "" {
		level = env
	}

	level = strings.ToLower(level)
	switch level {
	case "report":
		l.SetLevel(logrus.InfoLevel)
	default:
		if lvl, err := logrus.ParseLevel(level); err == nil {
			l.SetLevel(lvl)
		} else {
			return fmt.Errorf("invalid log level '%s'", level)
		}
	}

	// Ensure caller info is included
	l.SetReportCaller(true)

	callerPrettyfier := func(f *runtime.Frame) (string, string) {
		file := filepath.Base(f.File)
		return "", fmt.Sprintf("%s:%d", file, f.Line)
	}

	// Set formatter
	switch format {
	case "json":
		l.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339Nano,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
			CallerPrettyfier: callerPrettyfier,
		})
	case "text":
		l.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:    true,
			TimestampFormat:  time.RFC3339,
			CallerPrettyfier: callerPrettyfier,
		})
	default:
		return fmt.Errorf("invalid log format '%s'", format)
	}

	// Set output
	switch output {
	case "stdout", "":
		l.SetOutput(os.Stdout)
	case "stderr":
		l.SetOutput(os.Stderr)
	default:
		// Assume it's a file path
		if maxAge > 0 {
			l.SetOutput(&lumberjack.Logger{
				Filename: output,
				MaxAge:   maxAge,
				MaxSize:  100,
				Compress: true,
			})
		} else {
			file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				return fmt.Errorf("failed to open log file '%s': %w", output, err)
			}
			l.SetOutput(file)
		}
	}

	return nil
}

// Performance logging helper
func LogPerformanceEntry(entry *Entry, component string, operation string, duration time.Duration, fields Fields) {
	if fields == nil {
		fields = make(Fields)
	}
	fields["duration_ms"] = float64(duration.Nanoseconds()) / 1e6
	fields["operation"] = operation

	entry.WithFields(fields).WithComponent(component).Info("performance metric")
}

// Data flow logging helper
func LogDataFlowEntry(entry *Entry, source string, destination string, recordCount int, dataType string) {
	entry.WithFields(Fields{
		"source":       source,
		"destination":  destination,
		"record_count": recordCount,
		"data_type":    dataType,
		"flow_type":    "data_flow",
	}).Info("data flow metric")
}

// Metric logging helper
func (l *Log) LogMetric(component string, metric string, value interface{}, metricType string, fields Fields) {
	l.WithComponent(component).LogMetric(component, metric, value, metricType, fields)
	if fields == nil {
		fields = make(Fields)
	}
	if metricType == "" {
		metricType = "counter"
	}
	fields["metric"] = metric
	fields["value"] = value
	fields["metric_type"] = metricType

	l.WithComponent(component).WithFields(fields).Info("metric")
}

// Set output for logger
func (l *Log) SetOutput(output io.Writer) {
	l.Logger.SetOutput(output)
}

// Set level for logger
func (l *Log) SetLevel(level logrus.Level) {
	l.Logger.SetLevel(level)
}

// Set formatter for logger
func (l *Log) SetFormatter(formatter logrus.Formatter) {
	l.Logger.SetFormatter(formatter)
}

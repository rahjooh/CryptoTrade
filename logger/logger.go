package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// Fields type alias for logrus.Fields to maintain compatibility
type Fields map[string]interface{}

// Logger wraps logrus.Logger with additional functionality
type Logger struct {
	*logrus.Logger
}

// Entry wraps logrus.Entry with additional functionality
type Entry struct {
	*logrus.Entry
}

var globalLogger *Logger

func init() {
	globalLogger = NewLogger()
}

func NewLogger() *Logger {
	logger := logrus.New()

	// Set default formatter
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})

	// Set default level
	logger.SetLevel(logrus.InfoLevel)

	return &Logger{Logger: logger}
}

func GetLogger() *Logger {
	return globalLogger
}

func (l *Logger) WithComponent(component string) *Entry {
	return &Entry{Entry: l.Logger.WithField("component", component)}
}

func (l *Logger) WithFields(fields Fields) *Entry {
	return &Entry{Entry: l.Logger.WithFields(logrus.Fields(fields))}
}

func (l *Logger) WithError(err error) *Entry {
	return &Entry{Entry: l.Logger.WithError(err)}
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

// Convert Entry methods to return our Entry type
func (e *Entry) Info(args ...interface{}) {
	e.Entry.Info(args...)
}

func (e *Entry) Debug(args ...interface{}) {
	e.Entry.Debug(args...)
}

func (e *Entry) Warn(args ...interface{}) {
	e.Entry.Warn(args...)
}

func (e *Entry) Error(args ...interface{}) {
	e.Entry.Error(args...)
}

func (e *Entry) Fatal(args ...interface{}) {
	e.Entry.Fatal(args...)
}

func (e *Entry) Panic(args ...interface{}) {
	e.Entry.Panic(args...)
}

// LogMetric method for Entry
func (e *Entry) LogMetric(component string, metric string, value interface{}, fields Fields) {
	if fields == nil {
		fields = make(Fields)
	}
	fields["metric"] = metric
	fields["value"] = value
	fields["metric_type"] = "counter"

	e.WithComponent(component).WithFields(fields).Info("metric")
}

// Configure sets up the logger with the provided configuration
func (l *Logger) Configure(level string, format string, output string) error {
	// Set level
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("invalid log level '%s': %w", level, err)
	}
	l.SetLevel(logLevel)

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
		})
	case "text":
		l.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
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
		file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return fmt.Errorf("failed to open log file '%s': %w", output, err)
		}
		l.SetOutput(file)
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
func (l *Logger) LogMetric(component string, metric string, value interface{}, fields Fields) {
	if fields == nil {
		fields = make(Fields)
	}
	fields["metric"] = metric
	fields["value"] = value
	fields["metric_type"] = "counter"

	l.WithComponent(component).WithFields(fields).Info("metric")
}

// Set output for logger
func (l *Logger) SetOutput(output io.Writer) {
	l.Logger.SetOutput(output)
}

// Set level for logger
func (l *Logger) SetLevel(level logrus.Level) {
	l.Logger.SetLevel(level)
}

// Set formatter for logger
func (l *Logger) SetFormatter(formatter logrus.Formatter) {
	l.Logger.SetFormatter(formatter)
}

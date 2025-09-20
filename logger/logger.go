package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

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

func enableReportLevel(l *Log) {}

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
	reportMode := false
	if lvl, err := logrus.ParseLevel(strings.ToLower(levelStr)); err == nil {
		logger.SetLevel(lvl)
	} else {
		logger.SetLevel(logrus.InfoLevel)
		reportMode = true
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

	log := &Log{Logger: logger}
	if reportMode {
		enableReportLevel(log)
	}
	return log
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

func (e *Entry) Warn(args ...interface{}) { e.Entry.Warn(args...) }

func (e *Entry) Debug(args ...interface{}) {
	e.Entry.Debug(args...)
}

func (e *Entry) Error(args ...interface{}) { e.Entry.Error(args...) }

// Configure sets up the logger with the provided configuration
func (l *Log) Configure(level string, format string, output string, maxAge int) error {
	if env := os.Getenv("LOG_LEVEL"); env != "" {
		level = env
	}

	level = strings.ToLower(level)
	reportMode := false
	switch level {
	case "report":
		l.SetLevel(logrus.InfoLevel)
		reportMode = true
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
	if reportMode {
		enableReportLevel(l)
	}

	return nil
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

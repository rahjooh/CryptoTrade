package logger

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var Log zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = time.RFC3339

	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05",
		PartsOrder: []string{
			zerolog.TimestampFieldName,
			zerolog.LevelFieldName,
			"caller",
			zerolog.MessageFieldName,
		},
		FormatCaller: func(i interface{}) string {
			return "[" + callerInfo(6) + "]"
		},
	}

	logFile, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open log file")
	}
	multiWriter := io.MultiWriter(consoleWriter, logFile)

	Log = zerolog.New(multiWriter).With().
		Timestamp().
		Caller().
		Logger().
		Hook(callerHook{})
}

type callerHook struct{}

func (h callerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	pc, file, line, ok := runtime.Caller(4)
	if !ok {
		return
	}

	funcName := runtime.FuncForPC(pc).Name()
	shortFile := file[strings.LastIndex(file, "/")+1:]

	e.Str("func", funcName)
	e.Str("file", shortFile)
	e.Int("line", line)
}

func callerInfo(depth int) string {
	_, file, line, ok := runtime.Caller(depth)
	if !ok {
		return "unknown"
	}
	short := file[strings.LastIndex(file, "/")+1:]
	return short + ":" + strconv.Itoa(line)
}

func Debug(format string, v ...interface{}) {
	Log.Debug().Msg(fmt.Sprintf(format, v...))
}

func Info(format string, v ...interface{}) {
	Log.Info().Msg(fmt.Sprintf(format, v...))
}

func Warn(format string, v ...interface{}) {
	Log.Warn().Msg(fmt.Sprintf(format, v...))
}

func Error(format string, v ...interface{}) {
	Log.Error().Msg(fmt.Sprintf(format, v...))
}

func Fatal(format string, v ...interface{}) {
	Log.Fatal().Msg(fmt.Sprintf(format, v...))
}

func Fatalf(format string, v ...interface{}) {
	Log.Fatal().Msg(fmt.Sprintf(format, v...))
}

func Infof(format string, v ...interface{}) {
	Info(format, v...)
}

func Errorf(format string, v ...interface{}) {
	Error(format, v...)
}

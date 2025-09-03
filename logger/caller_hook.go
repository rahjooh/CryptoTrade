package logger

import (
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

// callerHook adjusts the caller reported by logrus so it points
// to the original call site outside of the logger package.
type callerHook struct{}

// Levels returns all log levels for this hook.
func (h *callerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire sets the entry's Caller to the first frame outside of logrus
// and this package.
func (h *callerHook) Fire(entry *logrus.Entry) error {
	pcs := make([]uintptr, 16)
	// Skip runtime.Callers, this method, logrus internals and our wrappers.
	n := runtime.Callers(6, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		fn := frame.Function
		if strings.Contains(fn, "sirupsen/logrus") || strings.Contains(fn, "cryptoflow/logger") {
			continue
		}
		entry.Caller = &frame
		break
	}
	return nil
}

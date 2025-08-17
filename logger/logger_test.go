package logger

import (
	"bytes"
	"testing"
)

// Test that error-level logs increment the global error counter
func TestErrorCount(t *testing.T) {
	l := Logger()
	ResetErrorCount()
	var buf bytes.Buffer
	l.SetOutput(&buf)

	l.Error("boom")
	if got := GetErrorCount(); got != 1 {
		t.Fatalf("expected error count 1, got %d", got)
	}

	l.Warn("warn")
	if got := GetErrorCount(); got != 1 {
		t.Fatalf("warn should not increment error count, got %d", got)
	}
}

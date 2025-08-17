package internal

import (
	"bytes"
	"encoding/json"
	"testing"

	"cryptoflow/logger"
)

// Test that logChannelStats emits system metrics and error count
func TestLogChannelStatsIncludesSystemMetrics(t *testing.T) {
	log := logger.GetLogger()
	logger.ResetErrorCount()
	var buf bytes.Buffer
	log.SetOutput(&buf)

	c := NewChannels(1, 1)
	c.logChannelStats(log)

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to unmarshal log entry: %v", err)
	}

	if _, ok := entry["cpu_percent"]; !ok {
		t.Errorf("cpu_percent field missing")
	}
	if _, ok := entry["ram_used"]; !ok {
		t.Errorf("ram_used field missing")
	}
	if _, ok := entry["error_count"]; !ok {
		t.Errorf("error_count field missing")
	}
}

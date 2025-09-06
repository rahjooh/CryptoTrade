package channel

import (
	"context"
	"testing"
	"time"
)

func TestNewChannels(t *testing.T) {
	c := NewChannels(1, 1)
	if c.FOBS == nil || c.FOBD == nil {
		t.Fatalf("expected non-nil sub channels")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.StartMetricsReporting(ctx)
	time.Sleep(10 * time.Millisecond)
	cancel()
	c.Close()
}

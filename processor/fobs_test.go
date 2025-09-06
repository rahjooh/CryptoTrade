package processor

import (
	"context"
	"testing"
	"time"

	appconfig "cryptoflow/config"
	"cryptoflow/models"
)

func minimalConfig() *appconfig.Config {
	return &appconfig.Config{
		Processor: appconfig.ProcessorConfig{
			MaxWorkers:   1,
			BatchSize:    1,
			BatchTimeout: time.Millisecond,
		},
	}
}

func TestFlattenerStartStop(t *testing.T) {
	cfg := minimalConfig()
	raw := make(chan models.RawFOBSMessage)
	norm := make(chan models.BatchFOBSMessage)
	f := NewFlattener(cfg, raw, norm)
	ctx, cancel := context.WithCancel(context.Background())
	if err := f.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := f.Start(ctx); err == nil {
		t.Fatalf("expected error on second start")
	}
	cancel()
	f.Stop()
}

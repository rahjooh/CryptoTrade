package dashboard

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"

	"cryptoflow/logger"
)

func TestResourceSamplerCollectsSamples(t *testing.T) {
	log := logger.Logger()
	sampler := newResourceSampler(3, time.Millisecond*10, "/", log)

	// Stub the collectors to produce deterministic data without touching the host.
	originalCPU := cpuPercentFn
	originalMem := memoryStatsFn
	originalDisk := diskUsageFn
	t.Cleanup(func() {
		cpuPercentFn = originalCPU
		memoryStatsFn = originalMem
		diskUsageFn = originalDisk
	})

	cpuCalls := atomic.Int32{}
	cpuPercentFn = func(ctx context.Context, interval time.Duration) ([]float64, error) {
		cpuCalls.Add(1)
		return []float64{42.5}, nil
	}
	memoryStatsFn = func(ctx context.Context) (*mem.VirtualMemoryStat, error) {
		return &mem.VirtualMemoryStat{Used: 1024, Total: 2048, UsedPercent: 50}, nil
	}
	diskUsageFn = func(ctx context.Context, path string) (*disk.UsageStat, error) {
		return &disk.UsageStat{Used: 4096, Total: 8192, UsedPercent: 50}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sampler.start(ctx)

	// Wait for a few samples to be collected.
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		if time.Now().After(deadline) {
			t.Fatal("resource sampler did not collect samples in time")
		}
		if len(sampler.snapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	sampler.stop()

	snapshots := sampler.snapshot()
	if len(snapshots) == 0 {
		t.Fatal("expected at least one resource snapshot")
	}

	latest := snapshots[len(snapshots)-1]
	if latest.CPUPercent != 42.5 || latest.MemoryPct != 50 || latest.DiskPct != 50 {
		t.Fatalf("unexpected snapshot data: %#v", latest)
	}

	if cpuCalls.Load() == 0 {
		t.Fatal("expected cpu sampler to be invoked")
	}
}

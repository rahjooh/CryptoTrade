package dashboard

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"

	"cryptoflow/logger"
)

// resourceSnapshot captures a single sample of host level resource utilisation.
type resourceSnapshot struct {
	Timestamp   time.Time `json:"timestamp"`
	CPUPercent  float64   `json:"cpu_percent"`
	MemoryUsed  uint64    `json:"memory_used"`
	MemoryTotal uint64    `json:"memory_total"`
	MemoryPct   float64   `json:"memory_percent"`
	DiskUsed    uint64    `json:"disk_used"`
	DiskTotal   uint64    `json:"disk_total"`
	DiskPct     float64   `json:"disk_percent"`
}

type resourceSampler struct {
	mu       sync.RWMutex
	items    []resourceSnapshot
	limit    int
	interval time.Duration
	diskPath string

	cancel  context.CancelFunc
	running atomic.Bool
	wg      sync.WaitGroup
	log     *logger.Log
}

var (
	cpuPercentFn = func(ctx context.Context, interval time.Duration) ([]float64, error) {
		return cpu.PercentWithContext(ctx, interval, false)
	}
	memoryStatsFn = mem.VirtualMemoryWithContext
	diskUsageFn   = disk.UsageWithContext
)

func newResourceSampler(limit int, interval time.Duration, diskPath string, log *logger.Log) *resourceSampler {
	if limit <= 0 {
		limit = 200
	}
	if interval <= 0 {
		interval = time.Second
	}
	if diskPath == "" {
		diskPath = "/"
	}
	return &resourceSampler{
		limit:    limit,
		interval: interval,
		diskPath: diskPath,
		log:      log,
	}
}

func (s *resourceSampler) start(ctx context.Context) {
	if s == nil {
		return
	}
	if s.running.Swap(true) {
		return
	}
	childCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(childCtx)
	}()
}

func (s *resourceSampler) stop() {
	if s == nil {
		return
	}
	if cancel := s.cancel; cancel != nil {
		cancel()
	}
	s.wg.Wait()
	s.running.Store(false)
}

func (s *resourceSampler) snapshot() []resourceSnapshot {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]resourceSnapshot, len(s.items))
	copy(out, s.items)
	return out
}

func (s *resourceSampler) append(snapshot resourceSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, snapshot)
	if len(s.items) > s.limit {
		s.items = append([]resourceSnapshot(nil), s.items[len(s.items)-s.limit:]...)
	}
}

func (s *resourceSampler) run(ctx context.Context) {
	defer s.running.Store(false)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cpuSamples, err := cpuPercentFn(ctx, s.interval)
		if err != nil {
			s.log.WithComponent("resource_sampler").WithError(err).Debug("failed to sample cpu usage")
			continue
		}

		memStats, err := memoryStatsFn(ctx)
		if err != nil {
			s.log.WithComponent("resource_sampler").WithError(err).Debug("failed to sample memory usage")
			continue
		}

		diskStats, err := diskUsageFn(ctx, s.diskPath)
		if err != nil {
			s.log.WithComponent("resource_sampler").WithError(err).Debug("failed to sample disk usage")
			continue
		}

		snapshot := resourceSnapshot{
			Timestamp:   time.Now(),
			CPUPercent:  firstSample(cpuSamples),
			MemoryUsed:  memStats.Used,
			MemoryTotal: memStats.Total,
			MemoryPct:   memStats.UsedPercent,
			DiskUsed:    diskStats.Used,
			DiskTotal:   diskStats.Total,
			DiskPct:     diskStats.UsedPercent,
		}

		s.append(snapshot)
	}
}

func firstSample(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	return samples[0]
}

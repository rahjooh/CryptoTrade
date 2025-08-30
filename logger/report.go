package logger

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

type channelStat struct {
	messages int64
	bytes    int64
}

var (
	errorsDelta    int64
	errorsSnapshot int64
	warnsDelta     int64
	warnsSnapshot  int64
	deltaReads     int64
	snapshotReads  int64
	s3WritesDelta  int64
	s3WritesSnap   int64
	channels       sync.Map // map[string]*channelStat
)

func recordWarn(component string) {
	if strings.Contains(component, "delta") {
		atomic.AddInt64(&warnsDelta, 1)
	} else if strings.Contains(component, "snapshot") {
		atomic.AddInt64(&warnsSnapshot, 1)
	}
}

func recordError(component string) {
	if strings.Contains(component, "delta") {
		atomic.AddInt64(&errorsDelta, 1)
	} else if strings.Contains(component, "snapshot") {
		atomic.AddInt64(&errorsSnapshot, 1)
	}
}

func IncrementDeltaRead(size int) {
	atomic.AddInt64(&deltaReads, 1)
	recordChannel("delta_ws", size)
}

func IncrementSnapshotRead(size int) {
	atomic.AddInt64(&snapshotReads, 1)
	recordChannel("snapshot_rest", size)
}

func IncrementS3WriteDelta(size int64) {
	atomic.AddInt64(&s3WritesDelta, 1)
	recordChannel("s3_delta_write", int(size))
}

func IncrementS3WriteSnapshot(size int64) {
	atomic.AddInt64(&s3WritesSnap, 1)
	recordChannel("s3_snapshot_write", int(size))
}

func RecordChannelMessage(name string, size int) {
	recordChannel(name, size)
}

func recordChannel(name string, size int) {
	v, _ := channels.LoadOrStore(name, &channelStat{})
	cs := v.(*channelStat)
	atomic.AddInt64(&cs.messages, 1)
	atomic.AddInt64(&cs.bytes, int64(size))
}

func startReport(ctx context.Context, log *Log, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				logReport(log)
			}
		}
	}()
}

// StartReport begins periodic logging of system and channel statistics.
// It exposes the internal startReport function for use by other packages.
func StartReport(ctx context.Context, log *Log, interval time.Duration) {
	startReport(ctx, log, interval)
}

func logReport(log *Log) {
	cpuPercent, _ := cpu.Percent(0, false)
	memStats, _ := mem.VirtualMemory()
	diskStats, _ := disk.Usage("/")

	channelData := map[string]map[string]int64{}
	channels.Range(func(k, v any) bool {
		name := k.(string)
		cs := v.(*channelStat)
		channelData[name] = map[string]int64{
			"messages": atomic.LoadInt64(&cs.messages),
			"bytes":    atomic.LoadInt64(&cs.bytes),
		}
		return true
	})

	cpuPct := 0.0
	if len(cpuPercent) > 0 {
		cpuPct = cpuPercent[0]
	}

	log.WithComponent("report").WithFields(Fields{
		"errors_delta":       atomic.LoadInt64(&errorsDelta),
		"errors_snapshot":    atomic.LoadInt64(&errorsSnapshot),
		"warns_delta":        atomic.LoadInt64(&warnsDelta),
		"warns_snapshot":     atomic.LoadInt64(&warnsSnapshot),
		"delta_reads":        atomic.LoadInt64(&deltaReads),
		"snapshot_reads":     atomic.LoadInt64(&snapshotReads),
		"s3_writes_delta":    atomic.LoadInt64(&s3WritesDelta),
		"s3_writes_snapshot": atomic.LoadInt64(&s3WritesSnap),
		"goroutines":         runtime.NumGoroutine(),
		"cpu_percent":        cpuPct,
		"memory_mb":          int64(memStats.Used) / 1024 / 1024,
		"disk_mb":            int64(diskStats.Used) / 1024 / 1024,
		"channels":           channelData,
	}).Info("runtime report")
}

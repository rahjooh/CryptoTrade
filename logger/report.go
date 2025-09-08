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
	gnet "github.com/shirou/gopsutil/v3/net" //cloudwatch

	"github.com/aws/aws-sdk-go-v2/aws"                              //cloudwatch
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types" //cloudwatch
)

type channelStat struct {
	messages int64
	bytes    int64
}

var (
	errorsDelta     int64
	errorsSnapshot  int64
	warnsDelta      int64
	warnsSnapshot   int64
	deltaReads      int64
	snapshotReads   int64
	s3WritesDelta   int64
	s3WritesSnap    int64
	channels        sync.Map // map[string]*channelStat
	droppedMessages int64
	retryCount      int64
	parquetErrors   int64
	reportInterval  time.Duration
	lastReads       int64
	lastDropped     int64
	lastRetry       int64
	lastParquet     int64
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
	if strings.Contains(component, "parquet") {
		atomic.AddInt64(&parquetErrors, 1)
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

func IncrementDroppedMessages() {
	atomic.AddInt64(&droppedMessages, 1)
}

func IncrementRetryCount() {
	atomic.AddInt64(&retryCount, 1)
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
				logReport(ctx, log)
			}
		}
	}()
}

// StartReport begins periodic logging of system and channel statistics.
// It exposes the internal startReport function for use by other packages.
func StartReport(ctx context.Context, log *Log, interval time.Duration) {
	reportInterval = interval
	startReport(ctx, log, interval)
}

func logReport(ctx context.Context, log *Log) {
	cpuPercent, _ := cpu.Percent(0, false)
	memStats, _ := mem.VirtualMemory()
	diskStats, _ := disk.Usage("/")
	netStats, _ := gnet.IOCounters(false)
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

	bytesSent := uint64(0)
	bytesRecv := uint64(0)
	if len(netStats) > 0 {
		bytesSent = netStats[0].BytesSent
		bytesRecv = netStats[0].BytesRecv
	}

	fields := Fields{
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
		"net_bytes_sent":     int64(bytesSent),
		"net_bytes_recv":     int64(bytesRecv),
		"dropped_messages":   atomic.LoadInt64(&droppedMessages),
		"retry_count":        atomic.LoadInt64(&retryCount),
		"parquet_errors":     atomic.LoadInt64(&parquetErrors),
	}

	log.WithComponent("report").WithFields(fields).Info("runtime report")

	var data []cwtypes.MetricDatum

	reads := atomic.LoadInt64(&deltaReads) + atomic.LoadInt64(&snapshotReads)
	ingest := reads - lastReads
	lastReads = reads

	dropped := atomic.LoadInt64(&droppedMessages)
	droppedDelta := dropped - lastDropped
	lastDropped = dropped

	retries := atomic.LoadInt64(&retryCount)
	retryDelta := retries - lastRetry
	lastRetry = retries

	parquet := atomic.LoadInt64(&parquetErrors)
	parquetDelta := parquet - lastParquet
	lastParquet = parquet

	data = append(data,
		cwtypes.MetricDatum{MetricName: aws.String("CPUPercent"), Unit: cwtypes.StandardUnitPercent, Value: aws.Float64(cpuPct)},
		cwtypes.MetricDatum{MetricName: aws.String("MemoryMB"), Unit: cwtypes.StandardUnitMegabytes, Value: aws.Float64(float64(memStats.Used) / 1024 / 1024)},
		cwtypes.MetricDatum{MetricName: aws.String("DiskMB"), Unit: cwtypes.StandardUnitMegabytes, Value: aws.Float64(float64(diskStats.Used) / 1024 / 1024)},
		cwtypes.MetricDatum{MetricName: aws.String("ErrorsDelta"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["errors_delta"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("ErrorsSnapshot"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["errors_snapshot"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("WarnsDelta"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["warns_delta"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("WarnsSnapshot"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["warns_snapshot"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("DeltaReads"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["delta_reads"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("SnapshotReads"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["snapshot_reads"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("S3WritesDelta"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["s3_writes_delta"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("S3WritesSnapshot"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(fields["s3_writes_snapshot"].(int64)))},
		cwtypes.MetricDatum{MetricName: aws.String("NetBytesSent"), Unit: cwtypes.StandardUnitBytes, Value: aws.Float64(float64(bytesSent))},
		cwtypes.MetricDatum{MetricName: aws.String("NetBytesRecv"), Unit: cwtypes.StandardUnitBytes, Value: aws.Float64(float64(bytesRecv))},
		cwtypes.MetricDatum{MetricName: aws.String("IngestRate"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(ingest) / reportInterval.Seconds())},
		cwtypes.MetricDatum{MetricName: aws.String("DroppedMessages"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(droppedDelta))},
		cwtypes.MetricDatum{MetricName: aws.String("RetryCount"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(retryDelta))},
		cwtypes.MetricDatum{MetricName: aws.String("ParquetWriteErrors"), Unit: cwtypes.StandardUnitCount, Value: aws.Float64(float64(parquetDelta))},
	)

	for name, stats := range channelData {
		data = append(data,
			cwtypes.MetricDatum{
				MetricName: aws.String("Hadi-ChannelMessages"),
				Unit:       cwtypes.StandardUnitCount,
				Dimensions: []cwtypes.Dimension{{Name: aws.String("Channel"), Value: aws.String(name)}},
				Value:      aws.Float64(float64(stats["messages"])),
			},
			cwtypes.MetricDatum{
				MetricName: aws.String("Hadi-ChannelBytes"),
				Unit:       cwtypes.StandardUnitBytes,
				Dimensions: []cwtypes.Dimension{{Name: aws.String("Channel"), Value: aws.String(name)}},
				Value:      aws.Float64(float64(stats["bytes"])),
			},
		)
	}

	publishMetrics(ctx, data)
}

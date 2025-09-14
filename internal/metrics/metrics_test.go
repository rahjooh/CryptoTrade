package metrics

import (
	"testing"

	"cryptoflow/logger"
)

func TestReportFOBDProcessorMetrics(t *testing.T) {
	log := logger.GetLogger()
	sizes := FOBDProcessorMetrics{RawLen: 1, RawCap: 2, NormLen: 3, NormCap: 4}
	ReportFOBDProcessorMetrics(log, sizes)
}

func TestReportFOBSProcessorMetrics(t *testing.T) {
	log := logger.GetLogger()
	stats := FOBSProcessorMetrics{
		MessagesProcessed: 1,
		BatchesProcessed:  2,
		EntriesProcessed:  3,
		ErrorsCount:       0,
		ActiveBatches:     1,
		RawChannelLen:     1,
		RawChannelCap:     2,
		NormChannelLen:    1,
		NormChannelCap:    2,
	}
	ReportFOBSProcessorMetrics(log, stats)
}

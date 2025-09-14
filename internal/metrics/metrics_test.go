package metrics

import (
	"cryptoflow/logger"
	"testing"
)

func Test_report_fobd_proccesor_metrics(t *testing.T) {
	log := logger.GetLogger()
	sizes := fobd_proccesor_metrics{RawLen: 1, RawCap: 2, NormLen: 3, NormCap: 4}
	report_fobd_proccesor_metrics(log, sizes)
}

func Test_report_fobs_proccesor_metrics(t *testing.T) {
	log := logger.GetLogger()
	stats := fobs_proccesor_metrics{
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
	report_fobs_proccesor_metrics(log, stats)
}

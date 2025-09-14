package rate

import (
	"net/http"
	"testing"

	"cryptoflow/logger"
)

func TestReportKucoinSnapshotWeight(t *testing.T) {
	log := logger.GetLogger()
	header := http.Header{}
	header.Set("gw-ratelimit-remaining", "1990")
	header.Set("gw-ratelimit-reset", "1000")
	ReportKucoinSnapshotWeight(log, header, 0, 2000, "")
}

func TestReportKucoinWSWeight(t *testing.T) {
	log := logger.GetLogger()
	tracker := NewKucoinWSWeightTracker()
	tracker.RegisterOutgoing(50)
	tracker.RegisterConnectionAttempt()
	ReportKucoinWSWeight(log, tracker, "")
}

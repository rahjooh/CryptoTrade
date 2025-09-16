package fobd

import "testing"

func TestChannelsStats(t *testing.T) {
	ch := NewChannels(2, 2)
	ch.IncrementRawSent()
	ch.IncrementNormSent()
	ch.IncrementRawDropped()
	ch.IncrementNormDropped()
	stats := ch.GetStats()
	if stats.RawSent != 1 || stats.NormSent != 1 || stats.RawDropped != 1 || stats.NormDropped != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestChannelsStartAndClose(t *testing.T) {
	ch := NewChannels(1, 1)
	ch.Close()
}

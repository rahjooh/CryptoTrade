package channel

import "testing"

func TestNewChannels(t *testing.T) {
	c := NewChannels(1, 1)
	if c.FOBS == nil || c.FOBD == nil || c.Liq == nil || c.FOI == nil || c.PI == nil {
		t.Fatalf("expected non-nil sub channels")
	}
	c.Close()
}

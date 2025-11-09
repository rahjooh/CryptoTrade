package liq

import (
	"context"
	"testing"
	"time"

	"cryptoflow/internal/models"
)

func TestChannels_SendRaw(t *testing.T) {
	ch := NewChannels(1)
	defer ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg := models.RawLiquidationMessage{Exchange: "binance", Symbol: "BTCUSDT", Market: "liquidation"}
	if !ch.SendRaw(ctx, msg) {
		t.Fatalf("expected send to succeed")
	}
	if stats := ch.GetStats(); stats.RawSent != 1 {
		t.Fatalf("expected raw sent counter to be 1, got %d", stats.RawSent)
	}

	// buffer full should increment dropped counter
	if ch.SendRaw(ctx, msg) {
		t.Fatalf("expected send to fail due to full buffer")
	}
	if stats := ch.GetStats(); stats.RawDropped != 1 {
		t.Fatalf("expected raw dropped counter to be 1, got %d", stats.RawDropped)
	}
}

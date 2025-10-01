package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNormFOBSMessageJSON(t *testing.T) {
	msg := NormFOBSMessage{
		Exchange:     "binance",
		Symbol:       "BTCUSDT",
		Market:       "futures",
		Timestamp:    time.Unix(0, 0),
		LastUpdateID: 1,
		Side:         "bid",
		Price:        100.5,
		Quantity:     1.2,
		Level:        1,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out NormFOBSMessage
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Exchange != out.Exchange || msg.Symbol != out.Symbol || msg.Market != out.Market ||
		!msg.Timestamp.Equal(out.Timestamp) || msg.LastUpdateID != out.LastUpdateID ||
		msg.Side != out.Side || msg.Price != out.Price || msg.Quantity != out.Quantity || msg.Level != out.Level {
		t.Fatalf("round trip mismatch: %+v != %+v", msg, out)
	}
}

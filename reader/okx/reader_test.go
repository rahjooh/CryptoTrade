package okx

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appconfig "cryptoflow/config"
	fobdchan "cryptoflow/internal/channel/fobd"
	fobschan "cryptoflow/internal/channel/fobs"
	"cryptoflow/logger"
	"cryptoflow/models"
)

// minimalConfig returns a minimal configuration required for testing.
func minimalConfig() *appconfig.Config {
	return &appconfig.Config{
		Reader: appconfig.ReaderConfig{Timeout: time.Second},
		Source: appconfig.SourceConfig{
			Okx: appconfig.OkxSourceConfig{
				ConnectionPool: appconfig.ConnectionPoolConfig{MaxIdleConns: 1, MaxConnsPerHost: 1, IdleConnTimeout: time.Second},
				Future: appconfig.OkxFutureConfig{
					Orderbook: appconfig.OkxFutureOrderbookConfig{
						Snapshots: appconfig.OkxSnapshotConfig{Enabled: true, Limit: 1, IntervalMs: 1000},
						Delta:     appconfig.OkxDeltaConfig{Enabled: true, URL: "wss://example.com/ws"},
					},
				},
			},
		},
	}
}

func TestNewReaders(t *testing.T) {
	cfg := minimalConfig()
	fobsCh := fobschan.NewChannels(1, 1)
	if r := Okx_FOBS_NewReader(cfg, fobsCh, []string{"BTC-USDT-SWAP"}, ""); r == nil {
		t.Fatal("Okx_FOBS_NewReader returned nil")
	}
	fobdCh := fobdchan.NewChannels(1, 1)
	if r := Okx_FOBD_NewReader(cfg, fobdCh, []string{"BTC-USDT-SWAP"}, ""); r == nil {
		t.Fatal("Okx_FOBD_NewReader returned nil")
	}
}

func TestOkxFOBDHandleEvent(t *testing.T) {
	ch := fobdchan.NewChannels(1, 1)
	r := &Okx_FOBD_Reader{channels: ch, ctx: context.Background(), log: logger.GetLogger()}

	evt := orderBookEvent{}
	evt.Arg.InstID = "BTC-USDT-SWAP"
	evt.Action = "snapshot"
	evt.Data = []struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Ts   string     `json:"ts"`
	}{{
		Bids: [][]string{{"1", "2"}},
		Asks: [][]string{{"3", "4"}},
		Ts:   "1700000000000",
	}}
	r.handleEvent(&evt)

	select {
	case msg := <-ch.Raw:
		var resp models.OkxFOBDResp
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp.Symbol != "BTC-USDT-SWAP" || resp.Action != "snapshot" {
			t.Fatalf("unexpected response: %+v", resp)
		}
		if len(resp.Bids) != 1 || resp.Bids[0].Price != "1" || resp.Bids[0].Quantity != "2" {
			t.Fatalf("unexpected bids: %+v", resp.Bids)
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestOkxFOBDProcessMessage(t *testing.T) {
	ch := fobdchan.NewChannels(1, 1)
	r := &Okx_FOBD_Reader{channels: ch, ctx: context.Background(), log: logger.GetLogger()}

	raw := []byte(`{"arg":{"instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"bids":[["1","2"]],"asks":[["3","4"]],"ts":"1700000000000"}]}`)
	if !r.processMessage(nil, raw) {
		t.Fatal("processMessage returned false")
	}

	select {
	case <-ch.Raw:
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

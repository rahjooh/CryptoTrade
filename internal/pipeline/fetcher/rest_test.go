package fetcher_test

//
//import (
//	"context"
//	"fmt"
//	"net/http"
//	"net/http/httptest"
//	"testing"
//	"time"
//
//	"CryptoFlow/internal/pipeline/fetcher"
//)
//
//func TestFetchRESTSnapshot(t *testing.T) {
//	mockData := `{
//        "lastUpdateId": 123456,
//        "bids": [["100.1", "1.0"]],
//        "asks": [["101.0", "1.5"]]
//    }`
//
//	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		w.Header().Set("Content-Type", "application/json")
//		fmt.Fprintln(w, mockData)
//	}))
//	defer server.Close()
//
//	client := &http.Client{Timeout: 5 * time.Second}
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	body, err := fetcher.FetchRESTSnapshot(ctx, client, server.URL, "binance", "spot", "BTCUSDT")
//	if err != nil {
//		t.Fatalf("FetchRESTSnapshot failed: %v", err)
//	}
//
//	if string(body) == "" {
//		t.Error("Expected non-empty response body")
//	}
//}

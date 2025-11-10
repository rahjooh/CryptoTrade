package dashboard

import (
	"testing"

	"cryptoflow/config"
	"cryptoflow/logger"
)

func TestNormalizeAddress(t *testing.T) {
	cases := map[string]string{
		"":                               "0.0.0.0:8080",
		"  :9090  ":                      "0.0.0.0:9090",
		"localhost":                      "localhost:8080",
		"0.0.0.0:80":                     "0.0.0.0:80",
		"[::1]:443":                      "[::1]:443",
		"::1":                            "[::1]:8080",
		"*:8080":                         "0.0.0.0:8080",
		"http://13.200.112.203:8080":     "13.200.112.203:8080",
		"https://13.200.112.203":         "13.200.112.203:8080",
		"http://:7070":                   "0.0.0.0:7070",
		"tcp://localhost:5050":           "localhost:5050",
		"https://dashboard.example.com/": "dashboard.example.com:8080",
	}

	for input, want := range cases {
		if got := normalizeAddress(input); got != want {
			t.Fatalf("normalizeAddress(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNewServerNormalizesConfiguredAddress(t *testing.T) {
	cfg := config.DashboardConfig{Enabled: true, Address: ":9000"}
	log := logger.Logger()

	srv, err := NewServer(cfg, log)
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	if srv == nil {
		t.Fatal("expected dashboard server, got nil")
	}
	if got := srv.Address(); got != "0.0.0.0:9000" {
		t.Fatalf("server address = %q, want %q", got, "0.0.0.0:9000")
	}
	srv.cleanup()
}

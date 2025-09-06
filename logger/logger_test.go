package logger

import (
	"os"
	"testing"
)

func TestWithComponent(t *testing.T) {
	log := Logger()
	entry := log.WithComponent("test")
	if v, ok := entry.Entry.Data["component"]; !ok || v != "test" {
		t.Fatalf("component field missing: %v", entry.Entry.Data)
	}
}

func TestConfigureInvalidLevel(t *testing.T) {
	// Ensure environment variables do not override the provided level
	t.Setenv("LOG_LEVEL", "")

	log := Logger()
	if err := log.Configure("invalid", "json", "stdout", 0); err == nil {
		t.Fatalf("expected error for invalid level")
	}
}

func TestWithEnv(t *testing.T) {
	os.Setenv("FOO", "bar")
	log := Logger()
	entry := log.WithEnv("FOO")
	if v, ok := entry.Entry.Data["FOO"]; !ok || v != "bar" {
		t.Fatalf("env field not set: %v", entry.Entry.Data)
	}
}

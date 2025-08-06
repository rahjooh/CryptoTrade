// cmd/CryptoFlow/main.go
// @tag main, entrypoint
package main

import (
	"CryptoFlow/internal/config"
	"CryptoFlow/internal/logger"
	"CryptoFlow/internal/pipeline"
	"CryptoFlow/internal/writer"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// The main function of the application.
func main() {
	logger.Init("info")
	logger.Info("Application starting...")

	// @note Load configuration from the YAML file.
	// Use config.SourceConf as the type for the loaded configuration.
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		logger.Fatalf("Error loading config: %v", err)
	}

	flattenedOutput := make(chan interface{}, 15000)

	var wg sync.WaitGroup

	// @note Pass the correct config type to StartReaders.
	pipeline.StartReaders(cfg, flattenedOutput, &wg)
	writer.StartWriters(flattenedOutput, &wg)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Received termination signal, shutting down gracefully...")

	close(flattenedOutput)

	wg.Wait()
	logger.Info("All components have shut down. Exiting.")
}

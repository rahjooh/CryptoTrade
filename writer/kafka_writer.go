package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	kafka "github.com/segmentio/kafka-go"

	appconfig "cryptoflow/config"
	"cryptoflow/logger"
	"cryptoflow/models"
)

type KafkaWriter struct {
	config        *appconfig.Config
	flattenedChan <-chan models.FlattenedOrderbookBatch
	writer        *kafka.Writer
	ctx           context.Context
	wg            *sync.WaitGroup
	mu            sync.RWMutex
	running       bool
	log           *logger.Log
}

func NewKafkaWriter(cfg *appconfig.Config, flattenedChan <-chan models.FlattenedOrderbookBatch) (*KafkaWriter, error) {
	if len(cfg.Storage.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers not configured")
	}
	kw := &KafkaWriter{
		config:        cfg,
		flattenedChan: flattenedChan,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Storage.Kafka.Brokers...),
			Topic:    cfg.Storage.Kafka.Topic,
			Balancer: &kafka.LeastBytes{},
		},
		wg:  &sync.WaitGroup{},
		log: logger.GetLogger(),
	}
	kw.log.WithComponent("kafka_writer").WithFields(logger.Fields{
		"brokers": cfg.Storage.Kafka.Brokers,
		"topic":   cfg.Storage.Kafka.Topic,
	}).Debug("kafka writer initialized")
	return kw, nil
}

func (kw *KafkaWriter) Start(ctx context.Context) error {
	kw.mu.Lock()
	if kw.running {
		kw.mu.Unlock()
		return fmt.Errorf("kafka writer already running")
	}
	kw.running = true
	kw.ctx = ctx
	kw.mu.Unlock()

	kw.log.WithComponent("kafka_writer").Debug("starting kafka writer")

	kw.wg.Add(1)
	go kw.run()

	return nil
}

func (kw *KafkaWriter) run() {
	defer kw.wg.Done()

	for {
		select {
		case <-kw.ctx.Done():
			return
		case batch, ok := <-kw.flattenedChan:
			if !ok {
				return
			}
			data, err := json.Marshal(batch)
			if err != nil {
				kw.log.WithComponent("kafka_writer").WithError(err).Warn("failed to marshal batch")
				continue
			}
			msg := kafka.Message{
				Key:   []byte(batch.Symbol),
				Value: data,
			}
			if err := kw.writer.WriteMessages(kw.ctx, msg); err != nil {
				kw.log.WithComponent("kafka_writer").WithError(err).Warn("failed to write message")
			} else {
				kw.log.WithComponent("kafka_writer").WithFields(logger.Fields{
					"batch_id": batch.BatchID,
					"records":  batch.RecordCount,
				}).Debug("batch written to kafka")
			}
		}
	}
}

func (kw *KafkaWriter) Stop() {
	kw.mu.Lock()
	kw.running = false
	kw.mu.Unlock()

	kw.log.WithComponent("kafka_writer").Debug("stopping kafka writer")
	kw.writer.Close()
	kw.wg.Wait()
	kw.log.WithComponent("kafka_writer").Debug("kafka writer stopped")
}

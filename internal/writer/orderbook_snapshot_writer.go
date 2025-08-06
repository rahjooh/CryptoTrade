// internal/writer/writer.go
// @tag writer, sink, aws, s3, parquet, hive
package writer

import (
	"CryptoFlow/internal/logger"
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// s3Config holds the S3 bucket details.
type s3Config struct {
	BucketName string
	Region     string
}

// StartWriters starts a goroutine to handle hourly data batching and writing to S3.
func StartWriters(flattenedInput <-chan interface{}, wg *sync.WaitGroup) {
	logger.Info("Starting S3 data writer with hourly batching...")
	wg.Add(1)
	go writeData(flattenedInput, wg)
}

// writeData buffers incoming data hourly and writes it to S3 as Parquet files.
func writeData(flattenedInput <-chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	cfg := s3Config{
		BucketName: "crypto-trade-data-bucket",
		Region:     "us-east-1",
	}
	uploader, err := newS3Uploader(cfg)
	if err != nil {
		logger.Fatalf("Failed to create S3 uploader: %v", err) // This now correctly calls the new Fatalf function
	}

	dataBuffers := make(map[string][]interface{})
	var mu sync.Mutex

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case item, ok := <-flattenedInput:
			if !ok {
				logger.Info("Flattened data channel closed. Writing remaining data...")
				flushBuffers(&mu, dataBuffers, uploader, cfg.BucketName)
				return
			}
			mu.Lock()
			key := reflect.TypeOf(item).String()
			dataBuffers[key] = append(dataBuffers[key], item)
			mu.Unlock()

		case <-ticker.C:
			logger.Info("Hourly ticker fired. Flushing data buffers...")
			flushBuffers(&mu, dataBuffers, uploader, cfg.BucketName)
		}
	}
}

// flushBuffers writes all buffered data to S3 and clears the buffers.
func flushBuffers(mu *sync.Mutex, dataBuffers map[string][]interface{}, uploader *s3manager.Uploader, bucketName string) {
	mu.Lock()
	defer mu.Unlock()

	for key, data := range dataBuffers {
		if len(data) == 0 {
			continue
		}

		logger.Infof("Flushing %d records for data type %s", len(data), key)

		var buf bytes.Buffer

		firstItem := data[0]

		pWriter, err := writer.NewParquetWriterFromWriter(&buf, firstItem, 4)
		if err != nil {
			logger.Errorf("Failed to create parquet writer: %v", err)
			continue
		}

		// Correct way to set compression in modern parquet-go
		pWriter.CompressionType = parquet.CompressionCodec_SNAPPY

		for _, item := range data {
			if err = pWriter.Write(item); err != nil {
				logger.Errorf("Failed to write record to parquet buffer: %v", err)
			}
		}

		if err = pWriter.WriteStop(); err != nil {
			logger.Errorf("Parquet writer failed to stop: %v", err)
		}

		if err := uploadToS3(&buf, uploader, bucketName, key); err != nil {
			logger.Errorf("Failed to upload file to S3: %v", err)
		} else {
			logger.Infof("Successfully uploaded %d records of type %s to S3", len(data), key)
		}
	}

	for k := range dataBuffers {
		delete(dataBuffers, k)
	}
}

// newS3Uploader creates a new AWS S3 uploader instance.
func newS3Uploader(cfg s3Config) (*s3manager.Uploader, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: credentials.NewEnvCredentials(),
	})
	if err != nil {
		return nil, err
	}
	return s3manager.NewUploader(sess), nil
}

// uploadToS3 uploads a buffer to a specified S3 path.
func uploadToS3(buf *bytes.Buffer, uploader *s3manager.Uploader, bucketName string, dataTypeKey string) error {
	now := time.Now().UTC()
	keyPath := fmt.Sprintf("%s/year=%d/month=%02d/day=%02d/hour=%02d/%s.parquet",
		dataTypeKey,
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		uuid.NewString(),
	)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyPath),
		Body:   bytes.NewReader(buf.Bytes()),
	})

	return err
}

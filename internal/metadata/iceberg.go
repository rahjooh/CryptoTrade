package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"cryptoflow/logger"
)

// DataFile describes a single parquet file written by the pipeline.
type DataFile struct {
	Path        string         `json:"path"`
	FileSize    int64          `json:"file_size_in_bytes"`
	RecordCount int64          `json:"record_count"`
	Partition   map[string]any `json:"partition"`
	Timestamp   time.Time      `json:"-"`
}

// ManifestEntry mirrors the information kept in an Iceberg manifest file.
type ManifestEntry struct {
	Status   int      `json:"status"`
	DataFile DataFile `json:"data_file"`
}

// Snapshot holds minimal information required for time-travel queries.
type Snapshot struct {
	SnapshotID  int64  `json:"snapshot-id"`
	TimestampMs int64  `json:"timestamp-ms"`
	Manifest    string `json:"manifest-list"`
}

// TableMetadata represents the high level Iceberg table metadata file.
type TableMetadata struct {
	FormatVersion     int        `json:"format-version"`
	TableUUID         string     `json:"table-uuid"`
	Location          string     `json:"location"`
	CurrentSnapshotID int64      `json:"current-snapshot-id"`
	Snapshots         []Snapshot `json:"snapshots"`
}

// Generator incrementally builds Iceberg metadata for a table.
type Generator struct {
	baseDir   string
	location  string
	bucket    string
	prefix    string
	tableName string
	tableUUID string
	snapshots []Snapshot
	s3Client  *s3.Client
}

// NewGenerator returns a metadata generator rooted at baseDir and targeting S3 location.
func NewGenerator(baseDir, location, bucket, prefix, tableName string, s3c *s3.Client) *Generator {
	gen := &Generator{
		baseDir:   baseDir,
		location:  location,
		bucket:    bucket,
		prefix:    prefix,
		tableName: tableName,
		tableUUID: uuid.NewString(),
		s3Client:  s3c,
	}
	logger.GetLogger().WithComponent("metadata").WithFields(logger.Fields{
		"base_dir":   baseDir,
		"table_name": tableName,
		"table_uuid": gen.tableUUID,
		"location":   location,
	}).Debug("metadata generator initialized")
	return gen
}

// AddFile records a newly written parquet file and updates metadata.
func (g *Generator) AddFile(df DataFile) error {
	snapID := df.Timestamp.UnixNano()
	manifestFile := fmt.Sprintf("manifest-%d.json", snapID)
	manifestPath := filepath.Join(g.baseDir, "metadata", manifestFile)
	log := logger.GetLogger().WithComponent("metadata").WithFields(logger.Fields{
		"file_path":     df.Path,
		"snapshot_id":   snapID,
		"manifest_file": manifestFile,
	})

	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		log.WithError(err).Error("failed to create manifest directory")
		return err
	}
	entry := ManifestEntry{Status: 1, DataFile: df}
	b, err := json.Marshal([]ManifestEntry{entry})
	if err != nil {
		log.WithError(err).Error("failed to marshal manifest entry")
		return err
	}
	if err := os.WriteFile(manifestPath, b, 0o644); err != nil {
		log.WithError(err).Error("failed to write manifest file")
		return err
	}
	snapshot := Snapshot{
		SnapshotID:  snapID,
		TimestampMs: df.Timestamp.UnixMilli(),
		Manifest:    manifestFile,
	}
	g.snapshots = append(g.snapshots, snapshot)
	if err := g.uploadFile(manifestPath, g.s3Key(manifestFile)); err != nil {
		log.WithError(err).Warn("failed to upload manifest to S3")
	}
	log.Info("manifest written and snapshot recorded")
	return g.writeTableMetadata()
}

func (g *Generator) writeTableMetadata() error {
	if len(g.snapshots) == 0 {
		return nil
	}
	tm := TableMetadata{
		FormatVersion:     2,
		TableUUID:         g.tableUUID,
		Location:          g.location,
		CurrentSnapshotID: g.snapshots[len(g.snapshots)-1].SnapshotID,
		Snapshots:         g.snapshots,
	}
	metaPath := filepath.Join(g.baseDir, "metadata", "metadata.json")
	b, err := json.MarshalIndent(tm, "", "  ")
	if err != nil {
		logger.GetLogger().WithComponent("metadata").WithError(err).Error("failed to marshal table metadata")
		return err
	}
	if err := os.WriteFile(metaPath, b, 0o644); err != nil {
		logger.GetLogger().WithComponent("metadata").WithError(err).Error("failed to write table metadata")
		return err
	}
	if err := g.uploadFile(metaPath, g.s3Key("metadata.json")); err != nil {
		logger.GetLogger().WithComponent("metadata").WithError(err).Warn("failed to upload table metadata to S3")
	}
	logger.GetLogger().WithComponent("metadata").WithFields(logger.Fields{
		"snapshot_count": len(g.snapshots),
		"metadata_path":  metaPath,
	}).Info("table metadata updated")
	return nil
}

// WriteCatalogEntry creates a simple catalog entry pointing at the table metadata.
func (g *Generator) WriteCatalogEntry(catalogDir string) error {
	metaLoc := fmt.Sprintf("s3://%s/%s", g.bucket, g.s3Key("metadata.json"))
	entry := map[string]string{
		"name":              g.tableName,
		"metadata_location": metaLoc,
	}
	if err := os.MkdirAll(catalogDir, 0o755); err != nil {
		logger.GetLogger().WithComponent("metadata").WithError(err).Error("failed to create catalog directory")
		return err
	}
	path := filepath.Join(catalogDir, fmt.Sprintf("%s.json", g.tableName))
	b, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		logger.GetLogger().WithComponent("metadata").WithError(err).Error("failed to marshal catalog entry")
		return err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		logger.GetLogger().WithComponent("metadata").WithError(err).Error("failed to write catalog entry")
		return err
	}
	logger.GetLogger().WithComponent("metadata").WithFields(logger.Fields{
		"catalog_path": path,
	}).Info("catalog entry written")
	return nil
}

func (g *Generator) s3Key(name string) string {
	if g.prefix != "" {
		return filepath.ToSlash(filepath.Join(g.prefix, "metadata", name))
	}
	return filepath.ToSlash(filepath.Join("metadata", name))
}

func (g *Generator) uploadFile(localPath, key string) error {
	if g.s3Client == nil {
		return nil
	}
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	_, err = g.s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(g.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	return err
}

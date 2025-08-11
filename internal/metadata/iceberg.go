package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
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
	basePath  string
	tableName string
	tableUUID string
	snapshots []Snapshot
}

// NewGenerator returns a metadata generator rooted at basePath.
func NewGenerator(basePath, tableName string) *Generator {
	return &Generator{
		basePath:  basePath,
		tableName: tableName,
		tableUUID: uuid.NewString(),
	}
}

// AddFile records a newly written parquet file and updates metadata.
func (g *Generator) AddFile(df DataFile) error {
	snapID := df.Timestamp.UnixNano()
	manifestFile := fmt.Sprintf("manifest-%d.json", snapID)
	manifestPath := filepath.Join(g.basePath, "metadata", manifestFile)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		return err
	}
	entry := ManifestEntry{Status: 1, DataFile: df}
	b, err := json.Marshal([]ManifestEntry{entry})
	if err != nil {
		return err
	}
	if err := os.WriteFile(manifestPath, b, 0o644); err != nil {
		return err
	}
	snapshot := Snapshot{
		SnapshotID:  snapID,
		TimestampMs: df.Timestamp.UnixMilli(),
		Manifest:    manifestFile,
	}
	g.snapshots = append(g.snapshots, snapshot)
	return g.writeTableMetadata()
}

func (g *Generator) writeTableMetadata() error {
	if len(g.snapshots) == 0 {
		return nil
	}
	tm := TableMetadata{
		FormatVersion:     2,
		TableUUID:         g.tableUUID,
		Location:          g.basePath,
		CurrentSnapshotID: g.snapshots[len(g.snapshots)-1].SnapshotID,
		Snapshots:         g.snapshots,
	}
	metaPath := filepath.Join(g.basePath, "metadata", "metadata.json")
	b, err := json.MarshalIndent(tm, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath, b, 0o644)
}

// WriteCatalogEntry creates a simple catalog entry pointing at the table metadata.
func (g *Generator) WriteCatalogEntry(catalogDir string) error {
	metaLoc := filepath.Join(g.basePath, "metadata", "metadata.json")
	entry := map[string]string{
		"name":              g.tableName,
		"metadata_location": metaLoc,
	}
	if err := os.MkdirAll(catalogDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(catalogDir, fmt.Sprintf("%s.json", g.tableName))
	b, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

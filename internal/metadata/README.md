# Metadata Generation

This directory contains utilities for building [Apache Iceberg](https://iceberg.apache.org/) metadata from Parquet snapshot files.

## Key Types
- **DataFile** – describes a single Parquet file including partitions and record counts.
- **ManifestEntry** – wraps a `DataFile` and mirrors the contents of a manifest file.
- **Snapshot** – tracks individual table snapshots for time‑travel queries.
- **TableMetadata** – represents the table‑level `metadata.json` file.

## Generator

`Generator` incrementally builds the metadata files:
1. `AddFile` writes a manifest for each Parquet file and records a `Snapshot`.
2. `writeTableMetadata` aggregates snapshots into `metadata.json`.
3. `WriteCatalogEntry` creates a simple catalog entry pointing at the table metadata.

All operations emit structured logs via the shared `logger` package for easy debugging and auditing.

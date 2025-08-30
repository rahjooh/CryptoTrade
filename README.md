# CryptoFlow

CryptoFlow is a Go service that streams high‑frequency order book snapshots from exchanges and stores them as Parquet files in S3.  The reference implementation ships with a Binance futures reader and is designed to run continuously with minimal operational overhead.

---

## Architecture and Data Flow

```
┌─────────────┐   RawOrderbookMessage    ┌─────────────┐   FlattenedOrderbookBatch   ┌───────────┐
│ Binance API │ ───────────────────────▶ │   Reader    │ ───────────────────────────▶ │ Flattener │
└─────────────┘                         └─────────────┘                               └─────┬─────┘
                                                                                         │
                                                                                         ▼
                                                                                ┌────────────┐
                                                                                │  S3 Writer │
                                                                                └────────────┘
```

1. **Reader** – polls the Binance depth endpoint at the configured interval and emits a `RawOrderbookMessage`.
2. **Flattener** – converts each message into a `FlattenedOrderbookBatch`, expanding bids and asks into individual price levels.
3. **S3 Writer** – buffers batches per `exchange/market/symbol` and periodically flushes them to S3 as Parquet files.
4. **Channels** – provide back‑pressure aware communication between stages and expose lightweight metrics.

### Channels

| Channel | Direction | Data Type | Description |
|---------|-----------|-----------|-------------|
| `RawFOBSch` | Reader ▶ Flattener | `models.RawOrderbookMessage` | Full order‑book snapshot including timestamp, last update ID, bids and asks. |
| `NormFOBSch` | Flattener ▶ S3 Writer | `models.FlattenedOrderbookBatch` | Batch of flattened entries (`Exchange`, `Market`, `Symbol`, `Timestamp`, `LastUpdateID`, `Side`, `Price`, `Quantity`, `Level`). |

---

## Repository Layout

```
CryptoFlow/
├── config/                # configuration loading and validation
├── internal/              # channel definitions and monitoring
├── logger/                # zerolog wrapper
├── models/                # order book message and batch structs
├── processor/             # flattener implementation
├── reader/                # Binance futures depth reader
├── writer/                # S3 parquet writer
├── main.go                # application entrypoint
├── config.yml             # runtime configuration
├── .env.example           # sample AWS credentials
└── ...
```

---

## Configuration

All runtime options live in `config.yml`.  Key sections:

- `cryptoflow`: service name and version.
- `channels`: buffer sizes for the raw and flattened channels.
- `reader`: concurrency and retry controls for the exchange client.
- `processor`: batch size and timeout for the flattener.
- `source`: exchange endpoints to poll (e.g. `binance: future: orderbook`).
- `storage.s3`: toggle and tune S3 writes (bucket, partition format, compression, etc.).
- `writer.buffer`: control batching behaviour including separate
  `snapshot_flush_interval` and `delta_flush_interval`.
- `logging`: level, format and output destination.

Sensitive S3 credentials are not stored in YAML.  Provide them through an `.env` file or the environment:

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=...
S3_BUCKET=...
```

Copy `.env.example` to `.env` and populate with your values before running the application.

---

## Running

```bash
# Install dependencies and run tests
go build ./...
go test  ./...

# Start the service (uses config.yml by default)
go run main.go
```

On startup CryptoFlow will:

1. Load environment variables from `.env`.
2. Read `config.yml` and validate required fields.
3. Start the reader, flattener and (if enabled) the S3 writer.
4. Begin streaming snapshots until interrupted (`Ctrl+C`).

The S3 writer partitions data as:

```
exchange=<exchange>/market=<market>/symbol=<symbol>/year=YYYY/month=MM/day=DD/hour=HH/<file>.parquet
```

and flushes snapshot and delta buffers at their configured intervals:
`writer.buffer.snapshot_flush_interval` and
`writer.buffer.delta_flush_interval`.

---

## Development

Useful commands during development:

```bash
# Format and vet code (optional)
go fmt ./...

# Run unit tests
go test ./...

# Execute the service with a custom config
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=... S3_BUCKET=... \
  go run main.go -config config.yml
```

Logging is handled by `logger` which wraps [zerolog](https://github.com/rs/zerolog).  Channel statistics are emitted every 30 seconds.

---

## Graceful Shutdown

The main process listens for `SIGINT`/`SIGTERM`.  When received it:

1. Cancels the root context.
2. Stops the S3 writer, flattener and reader in order.
3. Waits up to 30 seconds for all goroutines to exit.

This ensures buffered data is flushed before the process terminates.

---

## License

CryptoFlow is released under the [Apache 2.0 License](LICENSE).

---

_“Built for speed, clarity, and observability.”_


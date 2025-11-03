# CryptoFlow

CryptoFlow is a Go service that streams high‑frequency order book snapshots from exchanges and stores them as Parquet files in S3.  The reference implementation ships with a Binance futures reader and is designed to run continuously with minimal operational overhead.

---

## Architecture and Data Flow

```
┌─────────────┐   RawFOBSMessage    ┌─────────────┐   BatchFOBSMessage   ┌───────────┐
│ Binance API │ ──────────────────▶ │   Reader    │ ───────────────────▶ │ Flattener │
└─────────────┘                     └─────────────┘                      └─────┬─────┘
                                                                               │
                                                                               ▼
                                                                         ┌────────────┐
                                                                         │  S3 Writer │
                                                                         └────────────┘
```

1. **Reader** – polls the Binance depth endpoint at the configured interval and emits a `RawFOBSMessage`.
2. **Flattener** – converts each message into a `BatchFOBSMessage`, expanding bids and asks into individual price levels.
3. **S3 Writer** – buffers batches per `exchange/market/symbol` and periodically flushes them to S3 as Parquet files.
4. **Channels** – provide back‑pressure aware communication between stages.

### Channels

| Channel | Direction | Data Type | Description |
|---------|-----------|-----------|-------------|
| `RawFOBSch` | Reader ▶ Flattener | `models.RawFOBSMessage` | Snapshot channel (`internal/channel/fobs`). Full order‑book snapshot including timestamp, last update ID, bids and asks. |
| `NormFOBSch` | Flattener ▶ S3 Writer | `models.BatchFOBSMessage` | Normalized snapshot channel (`internal/channel/fobs`). Batch of flattened entries (`Exchange`, `Market`, `Symbol`, `Timestamp`, `LastUpdateID`, `Side`, `Price`, `Quantity`, `Level`). |
| `RawFOBDch` | Delta Reader ▶ Delta Processor | `models.RawFOBDMessage` | Delta channel (`internal/channel/fobd`). Order‑book diff messages describing incremental updates. |
| `NormFOBDch` | Delta Processor ▶ Delta Writer | `models.BatchFOBDMessage` | Normalized delta channel (`internal/channel/fobd`). Batches of processed delta updates ready for persistence. |

---

## Repository Layout

```
CryptoFlow/
├── config/                # configuration files and loaders
│   ├── config.yml         # runtime configuration
│   └── ip_shards.yml      # per-IP symbol mappings
├── internal/channel/      # channel definitions
│   ├── fobs/              # snapshot channels
│   └── fobd/              # delta channels
├── logger/                # zerolog wrapper
├── models/                # order book message and batch structs
├── processor/             # flattener implementation
├── reader/                # Binance futures depth reader
├── writer/                # S3 parquet writer
├── main.go                # application entrypoint
├── .env.example           # sample AWS credentials
└── ...
```

---

## Configuration

All runtime options live in `config/config.yml`.  Key sections:

- `cryptoflow`: service name and version.
- `channels`: buffer sizes for the raw and flattened channels.
- `reader`: concurrency and retry controls for the exchange client.
- `processor`: batch size and timeout for the flattener.
- `source`: exchange endpoints to poll (e.g. `binance: future: orderbook`).
- `storage.s3`: toggle and tune S3 writes (bucket, partition format, compression, etc.).
- `writer.buffer`: control batching behaviour including separate
  `snapshot_flush_interval` and `delta_flush_interval`.
- `logging`: level, format and output destination.

Sensitive S3 credentials are not stored in YAML.  Provide them through your runtime environment (for example by exporting shell variables locally or injecting secrets in CI/CD):

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=...
#Provide the bucket name via the S3_BUCKET environment variable
S3_BUCKET=...
```

Copy `.env.example` to `.env` and populate with your values before running the application.

### CI/CD: Single `ENV` GitHub Secret

To simplify secrets management, the workflows support a single multi‑line repository secret named `ENV` that contains all parameters for both staging and production. Create `Settings → Secrets and variables → Actions → New repository secret` with name `ENV` and contents like:

```
# Required: AWS
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-south-1
S3_BUCKET=your-bucket

# Optional: logging and dashboard
LOG_LEVEL=INFO
DASHBOARD_NAME=Data

# SSH to hosts (staging and production)
EC2_USER=ubuntu
EC2_HOST_STAGE=ec2-1-2-3-4.compute.amazonaws.com
EC2_HOST_PROD=ec2-5-6-7-8.compute.amazonaws.com

# Base64 of the private key used for SSH (no line wraps)
# macOS:    base64 -i id_rsa | tr -d '\n'
# Linux:    base64 -w0 id_rsa
EC2_SSH_KEY_B64=LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQo...

# Optional overrides (workflows compute sane defaults)
# APP_DIR=/home/${EC2_USER}/cryptoflow

# For CloudWatch dashboard host widgets (either ID or Name per env)
# Use IDs for fastest resolution, or Names if you prefer tagging
PROD_INSTANCE_ID=i-0123456789abcdef0
STAGE_INSTANCE_ID=i-0abcdef0123456789
# PROD_INSTANCE_NAME=prod-collector
# STAGE_INSTANCE_NAME=stage-collector
```

Notes:

- The workflows load this secret at runtime and export variables to later steps. The `EC2_SSH_KEY_B64` value is decoded and passed to SSH actions; use unwrapped Base64 (no line breaks).
- You can delete individual repository secrets and variables after you switch to `ENV`.
- Production uses Podman Compose on the target host; staging uses Docker Compose.
- When running the Prod workflow via the Actions UI, enable the `refresh_dashboard` input to also rebuild the CloudWatch dashboard (or include `[CWdash]` in a push commit message).

---

## Running

```bash
# Install dependencies and run tests
go build ./...
go test  ./...

# Start the service (uses config/config.yml by default)
go run main.go
```


When running in Docker and sharding traffic across multiple Elastic IPs, ensure the container uses the host network so the secondary private addresses are available. The provided `infra/docker/docker-compose.yml` sets `network_mode: host`, allowing the application to bind each symbol to the IP defined in `config/ip_shards.yml`.

On startup CryptoFlow will:

1. Load environment variables from `.env`.
2. Read `config/config.yml` and validate required fields.
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
  go run main.go -config config/config.yml
```

Logging is handled by `logger` which wraps [zerolog](https://github.com/rs/zerolog).

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

| symbols      | binance | bybit | kucoin | okx |
| ------------ | ------- |-------|--------|-----|
| AAVEUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| ADAUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| ARBUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| AVAXUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| BCHUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| BNBUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| BONKUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| BTCUSDC      | &#9989; | &#9989; | &#9989; | &#x274c;|
| BTCUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| DOGEUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| DOTUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| ETHUSDC      | &#9989; | &#9989; | &#9989; | &#x274c; |
| ETHUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| FARTCOINUSDT | &#9989; | &#9989; | &#x274c; | &#x274c; |
| FTMUSDT      | &#x274c; | &#x274c; | &#x274c; | &#x274c; |
| HYPEUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| INJUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| LINKUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| LTCUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| NEARUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| ONDOUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| OPUSDT       | &#9989; | &#9989; | &#9989; | &#9989; |
| PEPEUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| SHIBUSDT     | &#9989; | &#9989; | &#9989; | &#9989; |
| SOLUSDC      | &#9989; | &#9989; | &#9989; | &#x274c; |
| SOLUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| SUIUSDC      | &#9989; | &#9989; | &#9989; | &#x274c; |
| SUIUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| TRXUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| UNIUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| WIFUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| XLMUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |
| XRPUSDC      | &#9989; | &#9989; | &#9989; | &#x274c; |
| XRPUSDT      | &#9989; | &#9989; | &#9989; | &#9989; |

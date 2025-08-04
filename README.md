# 🍣 CryptoFlow – High-Frequency Binance Orderbook Collector

**CryptoFlow** is a Go-based high-frequency data collector that captures the BTCUSDT order book snapshot from Binance every second. It splits the snapshot into `bids` and `asks`, stores them in hourly rotated Parquet files, and exposes Prometheus metrics for observability.

---

## 📦 Features

- 🧠 **Async, functional, and isolated architecture**
- 📁 **Hourly Parquet file storage** for `bids` and `asks`
- 🔁 **Writer pooling** for performance and memory safety
- 📈 **Prometheus metrics** exposed on port `2112`
- 📦 **Dockerized and Makefile-driven** for easy build/run
- ✅ **Graceful shutdown** and clean logging with `zerolog`

---

## 📂 Directory Structure

```
CryptoFlow/
├── cmd/CryptoFlow/main.go                 # App entrypoint
├── internal/
│   ├── config/config.go              # File paths, output dirs, HTTP client
│   ├── logger/logger.go              # Structured logging with zerolog
│   ├── metrics/metrics.go            # Prometheus metric registration
│   ├── model/
│   │   ├── snapshot.go               # Binance response models
│   │   └── orderbook_row.go         # Parquet row schema
│   ├── reader/fetcher.go            # HTTP fetch logic
│   └── writer/pool.go               # Writer pool, hourly parquet rotation
├── Dockerfile                        # Build image
├── docker-compose.yml               # Optional for orchestration
├── Makefile                          # CLI automation
├── go.mod / go.sum                   # Dependencies
└── README.md                         # ← This file
```

---

## ⚙️ How It Works

1. Every second:
   - Fetch order book snapshot from Binance
   - Split into `bids` and `asks`
2. Each entry is converted into a table row:
   - `timestamp`, `price`, `quantity`
3. Rows are written into Parquet files:
   - Stored hourly under `./data/bids/YYYY-MM-DD_HH.parquet`
   - And `./data/asks/YYYY-MM-DD_HH.parquet`
4. Prometheus metrics are updated
5. Logs are written using `zerolog`

---

## 🚀 Getting Started

### 🧰 Requirements

- Docker
- `make`

### 🔨 Build the Docker image

```bash
make docker-build
```

### ▶️ Run the container

```bash
make docker-run
```

This will:

- Run the collector
- Write output to `./data` on your host
- Expose metrics at [http://localhost:2112/metrics](http://localhost:2112/metrics)

---

## 📊 Metrics Exposed

| Metric                             | Description                                 |
|------------------------------------|---------------------------------------------|
| `CryptoFlow_snapshot_success_total`     | Number of successful orderbook snapshots    |
| `CryptoFlow_snapshot_errors_total`      | Number of failed fetch/write attempts       |
| `go_goroutines` / `go_memstats_*` | Runtime stats (GC, heap, threads, etc.)     |
| `process_*`                        | CPU, RAM, file descriptor stats             |

---

## 📁 Output File Structure

Files are stored under:

```
./data/
├── bids/
│   ├── 2025-07-29_08.parquet
│   ├── 2025-07-29_09.parquet
│   └── ...
└── asks/
    ├── 2025-07-29_08.parquet
    ├── 2025-07-29_09.parquet
    └── ...
```

Each file includes one hour of 1-second resolution data for the respective side.

---

## 📘 Code Highlights

- **Writer Pool**: Prevents memory leaks by managing file handles per hour.
- **Parquet Format**: Fast, columnar storage ideal for analytics.
- **Zerolog**: Low overhead structured logging.
- **Prometheus**: Native Go instrumentation and `/metrics` endpoint.
- **Graceful Shutdown**: Catches SIGINT and SIGTERM to flush & close writers.

---

## 🛠 Makefile Commands

```bash
make build          # Build CryptoFlow binary
make run            # Run locally (without Docker)
make docker-build   # Build Docker image
make docker-run     # Run Docker container (with volume + metrics port)
make clean          # Remove compiled binary
```

---

## 🔐 Notes on Production

- Consider rate limits on Binance API
- Add exponential backoff for retries
- Rotate or offload `.parquet` files to cloud/S3
- Add `/healthz` or `/readyz` endpoints
- Use Prometheus + Grafana to visualize metrics

---

## 🧪 Future Enhancements

- Snapshot duration histogram
- Retry & backoff logic
- S3 uploader for parquet files
- Unit tests with mock HTTP client
- Alerting on error spike or data gaps

---

## 🧠 Credits

Developed with ❤️ using Go, Zerolog, Prometheus, and Parquet.

---

> _Built for speed, clarity, and observability._
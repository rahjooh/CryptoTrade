# 🧊 Crypto Market Data Lake Pipeline

A high-resolution, real-time data ingestion pipeline written in **Go**, designed to fetch and persist crypto market data from multiple exchanges (Binance, Bybit, OKX, KuCoin, Coinbase, Kraken) into **Apache Iceberg** / **Delta Lake**-compatible Parquet files in **Amazon S3**.

---

## 🚀 Features

- Multi-exchange support
- High-frequency snapshots and deltas (1s and 100ms)
- Configurable via YAML
- Modular architecture
- Optimized for S3 storage layout
- Delta Lake / Iceberg compatible partitioning
- Data types: order books, contracts, liquidations, open interest, funding rates

---

## 📦 S3 Layout

All data is stored in this structure:

```
s3://your-bucket/
  exchange=<exchange>/
    market_type=<spot|future>/
      data_type=<orderbook_snapshot|orderbook_delta|...>/
        symbol=<SYMBOL>/
          year=<YYYY>/month=<MM>/day=<DD>/hour=<HH>/
            part-*.snappy.parquet
```

### Example:
```
s3://crypto-lake/
  exchange=binance/
    market_type=future/
      data_type=orderbook_snapshot/
        symbol=BTCUSDT/
          year=2025/month=08/day=05/hour=13/
            part-0000.snappy.parquet
```

---

## 📊 Table Schemas

### `orderbook_snapshot`
```sql
CREATE TABLE orderbook_snapshot (
  exchange STRING,
  market_type STRING,
  symbol STRING,
  timestamp_ms BIGINT,
  bids ARRAY<STRUCT<price DOUBLE, quantity DOUBLE>>,
  asks ARRAY<STRUCT<price DOUBLE, quantity DOUBLE>>
)
```

### `orderbook_delta`
```sql
CREATE TABLE IF NOT EXISTS cryptoflowdb.s3_table (
                                                     exchange        string,
                                                     market          string,
                                                     symbol          string,
                                                     timestamp       timestamp,
                                                     last_update_id  bigint,
                                                     side            string,
                                                     price           double,
                                                     quantity        double,
                                                     level           int
)
    PARTITIONED BY (
                       exchange,
                       market,
                       symbol,
                       hour(timestamp)
    )
    LOCATION 's3://test-raw-databucket/S3-table/'
    TBLPROPERTIES (
                      'table_type' = 'ICEBERG',
                      'format'     = 'parquet'
                  );
```

### `contracts`
```sql
CREATE TABLE contracts (
  exchange STRING,
  symbol STRING,
  contract_code STRING,
  contract_type STRING,
  base_asset STRING,
  quote_asset STRING,
  tick_size DOUBLE,
  contract_size DOUBLE,
  timestamp_ms BIGINT
)
```

### `liquidation`
```sql
CREATE TABLE liquidation (
  exchange STRING,
  market_type STRING,
  symbol STRING,
  side STRING,
  price DOUBLE,
  quantity DOUBLE,
  timestamp_ms BIGINT
)
```

### `open_interest`
```sql
CREATE TABLE open_interest (
  exchange STRING,
  symbol STRING,
  open_interest DOUBLE,
  timestamp_ms BIGINT
)
```

### `funding_rate`
```sql
CREATE TABLE funding_rate (
  exchange STRING,
  symbol STRING,
  funding_rate DOUBLE,
  funding_time TIMESTAMP,
  timestamp_ms BIGINT
)
```

---

## ⚙️ Configuration

All sources and settings are defined in a single YAML file:

```yaml
source:
  binance:
    spot:
      orderbook:
        snapshots:
          enabled: true
          url: https://api.binance.com/api/v3/depth
          interval_ms: 1000
          symbols: [ BTCUSDT, ETHUSDT ]
```

More example config is available in [`config.yml`](./config.yml).

---

## 🧪 Technologies

- Language: **Go**
- Output Format: **Parquet** (`snappy` compressed)
- Storage: **Amazon S3**
- Compatibility: **Iceberg**, **Delta Lake**
- Query Engines: **Athena**, **Trino**, **Spark**, **DuckDB**

---

## 🛠️ Development

```bash
git clone https://github.com/your-org/crypto-data-lake.git
cd crypto-data-lake
go build ./...
```

---

## 🧩 Roadmap

- [ ] Real-time Prometheus metrics
- [ ] Web-based ingestion monitoring dashboard
- [ ] Stream compaction and small file management
- [ ] Kafka output support
- [ ] Multi-region S3 support

---

## 📜 License

MIT License

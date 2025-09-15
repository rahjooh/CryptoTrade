# Rate Limit Metrics

The `rate` package collects per‑IP rate‑limit usage for every supported exchange.
Readers call its helpers after each REST or WebSocket request to capture how many
requests have been consumed by that address.

1. **Configuration** – Free‑tier limits are declared in `config.yml` under
   `exchange_rate_limit` (e.g. Binance `REQUEST_WEIGHT: 2400`). These values become
   the gauge maximum on the CloudWatch dashboard.
2. **IP Shards** – Nine Elastic IPs are listed in `config/ip_shards.yml`. Each
   reader passes its bound IP to the rate helpers so metrics can be emitted and
   visualised per address.
3. **Capture** – Functions such as `ReportSnapshotWeight` (Binance) or
   `ReportKucoinSnapshotWeight` parse the weight headers and compute the `used`
   quota for that IP.
4. **Log** – Metrics are logged via `logger.LogMetric` with the exchange component
   and IP dimension.
5. **Publish** – The logger forwards numeric metrics to CloudWatch using
   `PutMetricData`.
6. **Visualize** – Dashboard gauge widgets plot `used_weight` against the
   configured maximum to show current utilisation per IP.

## Flow

```mermaid
flowchart TD
    Start[Start request] --> ParseHeaders[Parse rate-limit headers]
    ParseHeaders --> Compute[Compute used weight]
    Compute --> LogMetrics[Log metrics with logger]
    LogMetrics --> Publish[Send to CloudWatch]
    Publish --> Gauge[Dashboard gauges]
```

## Exchange Data Sequences

### Binance

Each REST or websocket request is bound to one of the nine shard IPs. The reader
passes that IP to `ReportSnapshotWeight`, which compares Binance’s
`X-MBX-USED-WEIGHT-1m` header against the configured per‑IP limit.

```mermaid
sequenceDiagram
    participant R as Binance reader
    participant Rate as ReportSnapshotWeight
    participant L as Logger
    participant CW as CloudWatch
    participant D as Dashboard

    R->>Rate: HTTP resp + IP
    Rate->>Rate: Read X-MBX-USED-WEIGHT-1m
    Rate->>L: used vs limit
    L->>CW: PutMetricData (ip)
    CW-->>D: Metric stored
    D-->>User: Gauge per IP
```

### Bybit

Bybit readers forward the originating IP so `ReportBybitSnapshotWeight` can
record usage against that address using the `X-Bapi-Limit` headers.

```mermaid
sequenceDiagram
    participant R as Bybit reader
    participant Rate as ReportBybitSnapshotWeight
    participant L as Logger
    participant CW as CloudWatch
    participant D as Dashboard

    R->>Rate: HTTP resp + IP
    Rate->>Rate: Read X-Bapi-Limit & Status
    Rate->>L: used weight
    L->>CW: PutMetricData (ip)
    CW-->>D: Metric stored
    D-->>User: Gauge per IP
```

### KuCoin

`ReportKucoinSnapshotWeight` receives the shard IP and subtracts the
`gw-ratelimit-remaining` header from the configured limit to compute usage per
address.

```mermaid
sequenceDiagram
    participant R as KuCoin reader
    participant Rate as ReportKucoinSnapshotWeight
    participant L as Logger
    participant CW as CloudWatch
    participant D as Dashboard

    R->>Rate: HTTP resp + IP
    Rate->>Rate: Read gw-ratelimit-remaining
    Rate->>L: used weight
    L->>CW: PutMetricData (ip)
    CW-->>D: Metric stored
    D-->>User: Gauge per IP
```

### OKX

OKX snapshot readers register each request with an `OkxRESTWeightTracker` tied
to the IP; metrics are emitted from `ReportOkxSnapshotWeight` for that address.

```mermaid
sequenceDiagram
    participant R as OKX reader
    participant T as OkxRESTWeightTracker
    participant Rate as ReportOkxSnapshotWeight
    participant L as Logger
    participant CW as CloudWatch
    participant D as Dashboard

    R->>T: RegisterRequest(IP)
    T->>Rate: used weight
    Rate->>L: used_weight
    L->>CW: PutMetricData (ip)
    CW-->>D: Metric stored
    D-->>User: Gauge per IP
```

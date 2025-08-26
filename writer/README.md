# S3 Tables Writer

This package provides a writer that sends flattened order book data directly to an Amazon S3 Table using the Iceberg REST endpoint. The writer authenticates requests with SigV4 (signing name `s3tables`) and avoids Glue, Athena and DynamoDB dependencies.

## Components

- **Initialization** – `NewS3Writer` configures the AWS SDK, parses the target table ARN and ensures the table bucket, namespace and table exist.
- **Direct Writes** – each batch is written immediately to the S3 Tables endpoint, signed with SigV4 so only S3 Tables charges apply (storage, requests, monitoring and optional compaction).
- **Workers & Metrics** – concurrent workers process incoming batches while periodic metrics reporting provides observability.

Each part is essential to reliably persist streaming data directly into an S3 Table without incurring Glue or Athena costs.

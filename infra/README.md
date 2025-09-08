# Infrastructure Provisioning

This directory contains Terraform configurations for setting up an S3 Tables table bucket, namespace and Iceberg table.

## Makefile usage

Run the following targets from within this directory:

- `make init` – initialize Terraform.
- `make import-bucket` – import the existing table bucket.
- `make import-namespace` – import the existing namespace.
- `make import-table` – import the existing table.
- `make replace-table` – recreate only the table.
- `make replace-namespace` – recreate the namespace and table.
- `make replace-bucket` – recreate the table bucket (destructive).

## Recommendations

- Review resource ARNs and region variables before running imports.
- Run `terraform fmt` and `terraform validate` to catch mistakes before applying.
- Replacements can delete data; be sure backups and state are in place before running destructive targets.


## CloudWatch Dashboard

A sample CloudWatch dashboard definition for the collector is available in `cloudwatch/collector-dashboard.json`.

To deploy it:

1. Replace `<collector-instance-id>` with the actual instance ID or other relevant dimensions.
2. Run:
   ```bash
   aws cloudwatch put-dashboard --dashboard-name Data --dashboard-body file://cloudwatch/collector-dashboard.json
   ```
3. Open the CloudWatch console and navigate to **Dashboards → Data**.

You can also have `docker-compose` apply the dashboard during deployment:

```bash
docker-compose up dashboard
```

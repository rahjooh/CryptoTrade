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

# Makefile Help — Terraform + AWS S3 Tables

This document explains how to use the provided **Makefile** to manage **AWS S3 Tables** infrastructure with Terraform and to inspect/delete **table buckets**, **namespaces**, and **tables** via the AWS CLI.

It’s designed to be:

- **Step-by-step** for Terraform (`init → fmt → validate → plan → apply`)
- **Introspective** (shows outputs and derived environment)
- **Safe by default** (your Terraform uses `prevent_destroy` in `main.tf`)
- **Practical** (includes “nuke” commands that delete in the correct order)

---

## Prerequisites

- **Terraform** ≥ 1.6  
- **AWS CLI v2** with `s3tables` commands (`aws s3tables help`)  
- **jq**  
- **Bash** available at `/bin/bash` (the Makefile sets `SHELL := /bin/bash`)

Authenticate to AWS as usual (env vars, SSO, or shared credentials).

---

## Configure Terraform variables

Set via `terraform.tfvars` (recommended) or `TF_VAR_*` env vars:

```hcl
# terraform.tfvars (example)
region            = "ap-south-1"
table_bucket_name = "my-s3tables-bucket"
namespace         = "marketdata"
table_name        = "s3_table"
# writer_role_arns = ["arn:aws:iam::123456789012:role/my-writer-role"]
```

> The Makefile expects Terraform to write a local JSON file (default `s3tables-outputs.json`).  
> If you changed `outputs_path` in `variables.tf`, run `make … OUT=/path/to/that.json`.

---

## Quick start

```bash
make check            # verify tools & AWS identity
make init             # terraform init
make fmt              # terraform fmt -recursive
make validate         # terraform validate
make plan             # terraform plan
make apply            # create resources
make outputs          # show TF outputs + print s3tables-outputs.json
```

Inspect S3 Tables:

```bash
make list-table-buckets
make list-namespaces
make list-tables
make get-table
```

---

## Daily cheat sheet

```bash
make plan-out     # create a plan artifact (tfplan.bin)
make apply-plan   # apply the saved plan
make show         # print current state
make graph        # write graph.dot
make state-list   # list resources in state
make clean        # remove tfplan.bin
```

---

## Deleting resources (correct order)

AWS requires **tables → namespace → table bucket** deletion.

```bash
# Delete one specific table (defaults to table parsed from outputs)
make delete-table

# Or specify a different table name
make delete-table TABLE=other_table

# Delete ALL tables in the current namespace
make delete-all-tables

# Full teardown: tables → namespace → table bucket
make nuke-s3tables

# If you deleted via CLI and want to clean Terraform state:
make state-rm-s3tables
```

> **Note:** `terraform destroy` may be blocked by lifecycle `prevent_destroy` in your Terraform.  
> The “nuke” targets use the AWS CLI directly and then offer `state-rm-s3tables` to clean TF state.

---

## Outputs file

After `make apply`, Terraform writes a JSON the Makefile reads (default `s3tables-outputs.json`):

```json
{
  "region": "ap-south-1",
  "account_id": "123456789012",
  "table_bucket_arn": "arn:aws:s3tables:...",
  "namespace": "marketdata",
  "table_arn": "arn:aws:s3tables:...",
  "rest_endpoint": "https://s3tables.ap-south-1.amazonaws.com/iceberg/v1",
  "table_fqn": "s3tablescatalog/<bucket>.<namespace>.<table>"
}
```

See what the Makefile resolved:

```bash
make env
```

---

## Target reference

### Helper / Info

| Target         | What it does                                   | Notes / Examples                 |
|----------------|--------------------------------------------------|----------------------------------|
| `help`         | List available targets (default goal)           | `make` or `make help`            |
| `env`          | Echo values parsed from `$(OUT)`                | region, ARNs, namespace, table   |
| `tools`        | Check `terraform`, `aws`, `jq`                  |                                  |
| `tf-version`   | Show Terraform version                          |                                  |
| `aws-version`  | Show AWS CLI version                            |                                  |
| `aws-whoami`   | Show AWS caller identity                        | Uses `--region` from outputs     |

### Terraform lifecycle

| Target        | What it does                                    | Notes / Examples                              |
|---------------|---------------------------------------------------|-----------------------------------------------|
| `init`        | `terraform init`                                  |                                               |
| `fmt`         | `terraform fmt -recursive`                        |                                               |
| `validate`    | `terraform validate`                              |                                               |
| `plan`        | `terraform plan`                                  |                                               |
| `plan-out`    | `terraform plan -out=tfplan.bin`                  | artifact name configurable via `PLANFILE`     |
| `apply`       | `terraform apply -auto-approve`                   |                                               |
| `apply-plan`  | `terraform apply tfplan.bin`                      | requires `plan-out` first                     |
| `show`        | `terraform show`                                  |                                               |
| `outputs`     | `terraform output` + print `$(OUT)` if present    |                                               |
| `state-list`  | `terraform state list`                            |                                               |
| `graph`       | `terraform graph > graph.dot`                     |                                               |
| `destroy`     | `terraform destroy`                               | may be blocked by `prevent_destroy`           |
| `clean`       | Remove `tfplan.bin`                               | cleans local plan artifact                    |

### AWS S3 Tables — Inspect

| Target                | What it does                                     | Notes |
|-----------------------|--------------------------------------------------|-------|
| `list-table-buckets`  | List S3 **table buckets**                        | region-scoped |
| `list-namespaces`     | List namespaces in current table bucket          | needs `table_bucket_arn` from outputs |
| `list-tables`         | List tables in current namespace                 | needs `table_bucket_arn` & `namespace` |
| `get-table-bucket`    | Details for current table bucket                 | needs `table_bucket_arn` |
| `get-namespace`       | Details for current namespace                    | needs `table_bucket_arn` & `namespace` |
| `get-table`           | Details for current table (parsed from `table_fqn`) | needs `table_bucket_arn`, `namespace` |

### AWS S3 Tables — Delete

| Target                 | What it does                                                       | Notes |
|------------------------|--------------------------------------------------------------------|-------|
| `delete-table`         | Delete **one** table                                               | override with `TABLE=name` |
| `delete-all-tables`    | Delete **all** tables in the current namespace                     | must have perms |
| `wait-empty-namespace` | Poll until namespace has zero tables                               | used by `nuke-s3tables` |
| `delete-namespace`     | Delete the namespace (must be empty)                               | order matters |
| `delete-table-bucket`  | Delete the table bucket (must have no namespaces/tables)           | last step |
| `nuke-s3tables`        | Full teardown: tables → namespace → table bucket                   | orchestrated order |
| `state-rm-s3tables`    | Remove S3 Tables resources from Terraform state (after CLI delete) | avoid drift |

---

## Environment & overrides

You can override these at invocation time, e.g. `make outputs OUT=/tmp/custom.json`:

- `OUT` – path to outputs json (defaults to `s3tables-outputs.json`)  
- `PLANFILE` – plan artifact (defaults to `tfplan.bin`)  
- `TF` / `AWS` / `JQ` – command names (if installed in nonstandard paths)  
- `TABLE` – used by `delete-table` (defaults to the table parsed from `table_fqn`)

The Makefile also auto-derives:
- `REGION`, `TABLE_BUCKET_ARN`, `NAMESPACE`, `TABLE_ARN`, `TABLE_FQN` from `$(OUT)`  
- `AWS_REGION_FLAG` → `--region <region>` if present

---

## Safety notes

- Your Terraform sets **`prevent_destroy = true`** for table bucket and table—this protects production data.  
  If you truly want to destroy with Terraform, remove or toggle that lifecycle block first (apply the change), then run `make destroy`.
- The “nuke” commands use the AWS CLI directly and **will** delete resources if you have permissions.
- Ensure you’re operating in the intended **region** and **account** (`make aws-whoami`, `make env`).

---

## Troubleshooting

- **No `s3tables` in AWS CLI** → Update to AWS CLI v2 and verify with `aws s3tables help`.  
- **`Outputs file 's3tables-outputs.json' not found`** → Run `make apply` first (or set `OUT=...`).  
- **Permission errors (AccessDenied)** → Ensure your principal has S3 Tables permissions and correct region.  
- **`terraform destroy` fails** → Expected when lifecycle `prevent_destroy` is set. Use the nuke targets (CLI) or temporarily remove lifecycle protection.  
- **Bash missing** → Install bash (e.g., `apk add bash` on Alpine) or change the Makefile header to POSIX-only mode (remove `pipefail`).

---

## Examples

Provision and inspect:

```bash
make check
make init fmt validate plan apply
make outputs
make list-table-buckets
make list-namespaces
make list-tables
make get-table
```

Delete everything (CLI path), then clean Terraform state:

```bash
make nuke-s3tables
make state-rm-s3tables
```

Plan once, apply later:

```bash
make plan-out
# review plan file if desired
make apply-plan
```

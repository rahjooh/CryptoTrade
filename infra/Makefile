# Makefile for S3 Tables Terraform workflow + AWS CLI inspection
# Requirements: terraform >=1.6, awscli v2 (with s3tables), jq
# Optional: set OUT=... if you changed outputs_path in variables.tf

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:
.DEFAULT_GOAL := help

TF            ?= terraform
AWS           ?= aws
JQ            ?= jq
OUT           ?= s3tables-outputs.json
PLANFILE      ?= tfplan.bin

# Run everything via the Terraform role profile from ~/.aws/config
PROFILE       ?= Terraform
AWS_ENV       := AWS_PROFILE=$(PROFILE) AWS_SDK_LOAD_CONFIG=1

# Compose commands with env prefix (do NOT self-reference!)
TFCMD         := $(AWS_ENV) $(TF)
AWSCMD        := $(AWS_ENV) $(AWS)

# Derive values from the persisted outputs file (written by local_file)
HAS_OUT          := $(shell test -f "$(OUT)" && echo 1 || echo 0)
REGION           := $(shell [ "$(HAS_OUT)" = "1" ] && $(JQ) -r '.region'            "$(OUT)" || echo "")
TABLE_BUCKET_ARN := $(shell [ "$(HAS_OUT)" = "1" ] && $(JQ) -r '.table_bucket_arn'  "$(OUT)" || echo "")
NAMESPACE        := $(shell [ "$(HAS_OUT)" = "1" ] && $(JQ) -r '.namespace'         "$(OUT)" || echo "")
TABLE_ARN        := $(shell [ "$(HAS_OUT)" = "1" ] && $(JQ) -r '.table_arn'         "$(OUT)" || echo "")
TABLE_FQN        := $(shell [ "$(HAS_OUT)" = "1" ] && $(JQ) -r '.table_fqn'         "$(OUT)" || echo "")
# Parse table name from FQN s3tablescatalog/<bucket>.<namespace>.<table>
TABLE_NAME       := $(shell [ -n "$(TABLE_FQN)" ] && echo "$(TABLE_FQN)" | awk -F'[./]' '{print $$NF}' || echo "")
AWS_REGION_FLAG  := $(if $(REGION),--region $(REGION),)

# -------- Helpers --------
.PHONY: help env check tools tf-version aws-version aws-whoami
help: ## Show this help
	@awk 'BEGIN{FS=":.*##"; print "Usage: make <target>\n\nTargets:"} /^[a-zA-Z0-9_.-]+:.*##/{printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

env: ## Show resolved environment (from $(OUT))
	@echo "OUT                 = $(OUT)"
	@echo "REGION              = $(REGION)"
	@echo "TABLE_BUCKET_ARN    = $(TABLE_BUCKET_ARN)"
	@echo "NAMESPACE           = $(NAMESPACE)"
	@echo "TABLE_NAME          = $(TABLE_NAME)"
	@echo "TABLE_ARN           = $(TABLE_ARN)"

tools: ## Check required tools exist
	@command -v $(TF)  >/dev/null || { echo "terraform not found"; exit 1; }
	@command -v $(AWS) >/dev/null || { echo "awscli not found"; exit 1; }
	@command -v $(JQ)  >/dev/null || { echo "jq not found"; exit 1; }
	@echo "All tools present."

check: tools tf-version aws-version aws-whoami ## Run preflight checks

tf-version: ## Show Terraform version
	$(TFCMD) version

aws-version: ## Show AWS CLI version
	$(AWSCMD) --version

aws-whoami: ## Show AWS caller identity
	$(AWSCMD) sts get-caller-identity $(AWS_REGION_FLAG)

# -------- Terraform steps (run individually) --------
.PHONY: init fmt validate plan plan-out apply apply-plan show outputs state-list destroy clean graph
init: ## terraform init
	$(TFCMD) init

fmt: ## terraform fmt (recursive)
	$(TFCMD) fmt -recursive

validate: ## terraform validate
	$(TFCMD) validate

plan: ## terraform plan
	$(TFCMD) plan

plan-out: ## terraform plan -out=$(PLANFILE)
	$(TFCMD) plan -out=$(PLANFILE)

apply: ## terraform apply (auto-approve)
	$(TFCMD) apply -auto-approve

apply-plan: ## terraform apply $(PLANFILE)
	@test -f "$(PLANFILE)" || { echo "Missing $(PLANFILE). Run 'make plan-out' first."; exit 1; }
	$(TFCMD) apply "$(PLANFILE)"

show: ## terraform show (current state)
	$(TFCMD) show

outputs: ## terraform output + cat persisted $(OUT)
	$(TFCMD) output
	@echo "------- $(OUT) -------"
	@test -f "$(OUT)" && cat "$(OUT)" || echo "No outputs file yet. Run 'make apply'."

state-list: ## terraform state list
	$(TFCMD) state list

graph: ## terraform graph (DOT) -> graph.dot
	$(TFCMD) graph > graph.dot && echo "Wrote graph.dot"

destroy: ## terraform destroy (may be blocked by prevent_destroy in main.tf)
	@echo "NOTE: destroy may fail due to prevent_destroy=true in resources."
	-$(TFCMD) destroy

clean: ## Remove local plan file
	@rm -f "$(PLANFILE)"

# -------- S3 Tables CLI shortcuts (pull from $(OUT)) --------
.PHONY: list-table-buckets list-namespaces list-tables get-table-bucket get-namespace get-table
_require_out:
	@test -f "$(OUT)" || { echo "Outputs file '$(OUT)' not found. Run 'make apply' first or set OUT=..."; exit 1; }

list-table-buckets: ## List S3 Table Buckets in the account
	$(AWSCMD) s3tables list-table-buckets $(AWS_REGION_FLAG)

list-namespaces: _require_out ## List namespaces in the table bucket
	@test -n "$(TABLE_BUCKET_ARN)" || { echo "TABLE_BUCKET_ARN missing"; exit 1; }
	$(AWSCMD) s3tables list-namespaces --table-bucket-arn "$(TABLE_BUCKET_ARN)" $(AWS_REGION_FLAG)

list-tables: _require_out ## List tables (optionally filter by namespace)
	@test -n "$(TABLE_BUCKET_ARN)" || { echo "TABLE_BUCKET_ARN missing"; exit 1; }
	@if [ -n "$(NAMESPACE)" ]; then \
	  $(AWSCMD) s3tables list-tables --table-bucket-arn "$(TABLE_BUCKET_ARN)" --namespace "$(NAMESPACE)" $(AWS_REGION_FLAG); \
	else \
	  $(AWSCMD) s3tables list-tables --table-bucket-arn "$(TABLE_BUCKET_ARN)" $(AWS_REGION_FLAG); \
	fi

get-table-bucket: _require_out ## Get details for the table bucket
	@test -n "$(TABLE_BUCKET_ARN)" || { echo "TABLE_BUCKET_ARN missing"; exit 1; }
	$(AWSCMD) s3tables get-table-bucket --table-bucket-arn "$(TABLE_BUCKET_ARN)" $(AWS_REGION_FLAG)

get-namespace: _require_out ## Get details for the namespace
	@test -n "$(TABLE_BUCKET_ARN)" -a -n "$(NAMESPACE)" || { echo "TABLE_BUCKET_ARN/NAMESPACE missing"; exit 1; }
	$(AWSCMD) s3tables get-namespace --table-bucket-arn "$(TABLE_BUCKET_ARN)" --namespace "$(NAMESPACE)" $(AWS_REGION_FLAG)

get-table: _require_out ## Get details for the table (by name from FQN)
	@test -n "$(TABLE_BUCKET_ARN)" -a -n "$(NAMESPACE)" -a -n "$(TABLE_NAME)" || { echo "Need TABLE_BUCKET_ARN, NAMESPACE, TABLE_NAME"; exit 1; }
	$(AWSCMD) s3tables get-table --table-bucket-arn "$(TABLE_BUCKET_ARN)" --namespace "$(NAMESPACE)" --name "$(TABLE_NAME)" $(AWS_REGION_FLAG)

# ---------------- Deletion/Nuke targets (S3 Tables) ----------------
# Docs: delete-table, delete-namespace, delete-table-bucket and ordering notes:
# - Must delete tables before deleting a namespace. :contentReference[oaicite:0]{index=0}
# - Must delete namespaces/tables before deleting the table bucket. :contentReference[oaicite:1]{index=1}

.PHONY: delete-table delete-all-tables wait-empty-namespace delete-namespace delete-table-bucket nuke-s3tables state-rm-s3tables

# Optionally override TABLE on the command line: `make delete-table TABLE=my_table`
TABLE ?= $(TABLE_NAME)

delete-table: _require_out ## Delete a single table (TABLE?=$(TABLE_NAME))
	@test -n "$(TABLE_BUCKET_ARN)" -a -n "$(NAMESPACE)" -a -n "$(TABLE)" || { echo "Need TABLE_BUCKET_ARN, NAMESPACE, and TABLE"; exit 1; }
	@echo "Deleting table: $(NAMESPACE).$(TABLE)"
	$(AWSCMD) s3tables delete-table \
	  --table-bucket-arn "$(TABLE_BUCKET_ARN)" \
	  --namespace "$(NAMESPACE)" \
	  --name "$(TABLE)" \
	  $(AWS_REGION_FLAG)

delete-all-tables: _require_out ## Delete ALL tables in the namespace
	@test -n "$(TABLE_BUCKET_ARN)" -a -n "$(NAMESPACE)" || { echo "Need TABLE_BUCKET_ARN and NAMESPACE"; exit 1; }
	@echo "Listing tables in namespace: $(NAMESPACE)"
	@set -eu; \
	tables="$$( $(AWSCMD) s3tables list-tables --table-bucket-arn "$(TABLE_BUCKET_ARN)" --namespace "$(NAMESPACE)" --output json $(AWS_REGION_FLAG) | $(JQ) -r '.tables[]?.name // empty' )"; \
	if [ -z "$$tables" ]; then \
	  echo "No tables found in namespace $(NAMESPACE)."; \
	else \
	  for t in $$tables; do \
	    echo "Deleting $$t ..."; \
	    $(AWSCMD) s3tables delete-table --table-bucket-arn "$(TABLE_BUCKET_ARN)" --namespace "$(NAMESPACE)" --name "$$t" $(AWS_REGION_FLAG); \
	  done; \
	fi

wait-empty-namespace: _require_out ## Wait until namespace has zero tables
	@test -n "$(TABLE_BUCKET_ARN)" -a -n "$(NAMESPACE)" || { echo "Need TABLE_BUCKET_ARN and NAMESPACE"; exit 1; }
	@echo "Waiting for namespace $(NAMESPACE) to become empty..."
	@set -eu; \
	while :; do \
	  count="$$( $(AWSCMD) s3tables list-tables --table-bucket-arn "$(TABLE_BUCKET_ARN)" --namespace "$(NAMESPACE)" --output json $(AWS_REGION_FLAG) | $(JQ) -r '.tables | length' 2>/dev/null || echo 0)"; \
	  [ "$$count" -eq 0 ] && { echo "Namespace is empty."; break; } || { echo "Remaining tables: $$count"; sleep 3; }; \
	done

delete-namespace: _require_out ## Delete the namespace (must be empty)
	@test -n "$(TABLE_BUCKET_ARN)" -a -n "$(NAMESPACE)" || { echo "Need TABLE_BUCKET_ARN and NAMESPACE"; exit 1; }
	@echo "Deleting namespace: $(NAMESPACE)"
	$(AWSCMD) s3tables delete-namespace \
	  --table-bucket-arn "$(TABLE_BUCKET_ARN)" \
	  --namespace "$(NAMESPACE)" \
	  $(AWS_REGION_FLAG)

delete-table-bucket: _require_out ## Delete the table bucket (must have no namespaces/tables)
	@test -n "$(TABLE_BUCKET_ARN)" || { echo "Need TABLE_BUCKET_ARN"; exit 1; }
	@echo "Deleting table bucket: $(TABLE_BUCKET_ARN)"
	$(AWSCMD) s3tables delete-table-bucket \
	  --table-bucket-arn "$(TABLE_BUCKET_ARN)" \
	  $(AWS_REGION_FLAG)

nuke-s3tables: delete-all-tables wait-empty-namespace delete-namespace delete-table-bucket ## Delete tables → namespace → table bucket (in order)
	@echo "Nuke complete."

# Optional: clean up Terraform state after manual (CLI) deletion to avoid drift
state-rm-s3tables: ## Remove S3 Tables resources from Terraform state (after CLI deletion)
	-$(TFCMD) state rm awscc_s3tables_table.tbl
	-$(TFCMD) state rm awscc_s3tables_namespace.ns
	-$(TFCMD) state rm aws_s3tables_table_bucket.this
	@echo "Removed S3 Tables resources from Terraform state."

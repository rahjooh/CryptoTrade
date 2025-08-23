terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.2.0"
    }
    awscc = {
      source  = "hashicorp/awscc"
      version = ">= 1.40.0"
    }
    local = {
      source  = "hashicorp/local"
      version = ">= 2.5.1"
    }
  }

  # Optional remote backend:
  # backend "s3" {
  #   bucket         = "your-tf-state-bucket"
  #   key            = "s3tables/infra.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "your-tf-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region  = var.region
  profile = "default"
}

provider "awscc" {
  region  = var.region
  profile = "default"
}

provider "local" {}

data "aws_caller_identity" "current" {}

############################
# 1) S3 Tables: table bucket
############################
resource "aws_s3tables_table_bucket" "this" {
  name = var.table_bucket_name

  # encryption_configuration intentionally omitted (SSE-S3 is always on by default)

  lifecycle {
    # MUST be a literal, not a variable/expression
    prevent_destroy = true
  }
}

############################
# 2) Namespace
############################
resource "awscc_s3tables_namespace" "ns" {
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  namespace        = var.namespace
}

########################################
# 3) Table (ICEBERG) with schema
########################################
resource "awscc_s3tables_table" "tbl" {
  table_bucket_arn  = aws_s3tables_table_bucket.this.arn
  namespace         = var.namespace
  table_name        = var.table_name
  open_table_format = "ICEBERG"

  iceberg_metadata = {
    iceberg_schema = {
      schema_field_list = [
        { name = "exchange", type = "string", required = true },
        { name = "market", type = "string", required = true },
        { name = "symbol", type = "string", required = true },
        { name = "timestamp", type = "timestamp", required = true },
        { name = "last_update_id", type = "long", required = true },
        { name = "side", type = "string", required = true },
        { name = "price", type = "double", required = true },
        { name = "quantity", type = "double", required = true },
        { name = "level", type = "int", required = true }
      ]
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

########################################
# 4) Least-privilege writer policy (optional)
########################################
data "aws_iam_policy_document" "writer" {
  statement {
    sid = "S3TablesControlPlane"
    actions = [
      "s3tables:CreateNamespace",
      "s3tables:CreateTable",
      "s3tables:GetTableBucket",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
      "s3tables:GetNamespace",
      "s3tables:GetTable",
      "s3tables:UpdateTableMetadataLocation"
    ]
    resources = [
      aws_s3tables_table_bucket.this.arn,
      "${aws_s3tables_table_bucket.this.arn}/*"
    ]
  }

  statement {
    sid = "S3TablesDataPlaneForThisTable"
    actions = [
      "s3tables:PutTableData",
      "s3tables:GetTableData"
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "s3tables:namespace"
      values   = [var.namespace]
    }
    condition {
      test     = "StringEquals"
      variable = "s3tables:tableName"
      values   = [var.table_name]
    }
  }
}

resource "aws_iam_policy" "writer" {
  count  = length(var.writer_role_arns) > 0 ? 1 : 0
  name   = "${var.name_prefix}-s3tables-writer"
  policy = data.aws_iam_policy_document.writer.json
}

resource "aws_iam_role_policy_attachment" "attach" {
  for_each   = toset(var.writer_role_arns)
  role       = each.value
  policy_arn = aws_iam_policy.writer[0].arn
}

########################################
# 5) Persist outputs to a local JSON file
########################################
locals {
  rest_endpoint = "https://s3tables.${var.region}.amazonaws.com/iceberg/v1"

  outputs_map = {
    region           = var.region
    account_id       = data.aws_caller_identity.current.account_id
    table_bucket_arn = aws_s3tables_table_bucket.this.arn
    namespace        = awscc_s3tables_namespace.ns.namespace
    table_arn        = awscc_s3tables_table.tbl.table_arn
    rest_endpoint    = local.rest_endpoint
    table_fqn        = "s3tablescatalog/${var.table_bucket_name}.${var.namespace}.${var.table_name}"
  }
}

resource "local_file" "persist_outputs" {
  filename        = var.outputs_path
  content         = jsonencode(local.outputs_map)
  file_permission = "0644"
}

########################################
# 6) Human-friendly outputs
########################################
output "table_bucket_arn" { value = aws_s3tables_table_bucket.this.arn }
output "namespace" { value = awscc_s3tables_namespace.ns.namespace }
output "table_arn" { value = awscc_s3tables_table.tbl.table_arn }
output "rest_endpoint" { value = local.rest_endpoint }
output "outputs_file" { value = local_file.persist_outputs.filename }

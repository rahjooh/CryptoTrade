# -------------------------------------------------------------------
# provider configuration
# -------------------------------------------------------------------
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
  required_version = ">= 1.5"
}

provider "aws" {
  region = var.region
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "data_lake_bucket_name" {
  type        = string
  description = "Name of S3 bucket for table data"
}

variable "principal_arn" {
  type        = string
  description = "IAM role/user that writes/query the table"
}

# -------------------------------------------------------------------
# S3 bucket to store table data
# -------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name
}

# -------------------------------------------------------------------
# Glue database & table
# -------------------------------------------------------------------
resource "aws_glue_catalog_database" "lake_db" {
  name = "my_lake_db"
}

resource "aws_glue_catalog_table" "s3_table" {
  name          = "my_s3_table"
  database_name = aws_glue_catalog_database.lake_db.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/my_s3_table/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    columns {
      name = "id"
      type = "bigint"
    }
    columns {
      name = "name"
      type = "string"
    }

    serde_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }
}

# -------------------------------------------------------------------
# Lake Formation registration and permissions
# -------------------------------------------------------------------
resource "aws_lakeformation_resource" "s3_bucket" {
  arn  = aws_s3_bucket.data_lake.arn
  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/AWSServiceRoleForLakeFormationDataAccess"
}

data "aws_caller_identity" "current" {}

# Grant ALL permissions on the table to your execution role/user
resource "aws_lakeformation_permissions" "writer_permissions" {
  principal = var.principal_arn

  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.lake_db.name
    name          = aws_glue_catalog_table.s3_table.name
  }
}

# (optional) grant database-level permissions
resource "aws_lakeformation_permissions" "db_permissions" {
  principal = var.principal_arn

  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.lake_db.name
  }
}

# -------------------------------------------------------------------
# Athena workgroup for queries (optional but recommended)
# -------------------------------------------------------------------
resource "aws_athena_workgroup" "lake_wg" {
  name = "my_lake_wg"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena_results/"
    }
  }
}

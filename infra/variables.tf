variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "table_bucket_name" {
  description = "S3 Tables table-bucket name (3-63, lowercase, digits, hyphens)."
  type        = string
}

variable "namespace" {
  description = "Namespace (db-like)."
  type        = string
  default     = "marketdata"
}

variable "table_name" {
  description = "Table name."
  type        = string
  default     = "s3_table"
}

variable "writer_role_arns" {
  description = "Optional IAM roles to attach the writer policy to."
  type        = list(string)
  default     = []
}

variable "prevent_destroy" {
  description = "Safety switch to block accidental deletion in non-dev."
  type        = bool
  default     = true
}

variable "outputs_path" {
  description = "Where to write a JSON file with outputs."
  type        = string
  default     = "s3tables-outputs.json"
}

variable "name_prefix" {
  description = "Prefix used for named resources like IAM policies."
  type        = string
  default     = "raw"
}

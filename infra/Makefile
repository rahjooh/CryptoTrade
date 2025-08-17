.PHONY: init import-bucket import-namespace import-table replace-table replace-namespace replace-bucket

# 1) init
init:
	terraform init

# 2) import the table bucket
import-bucket:
	terraform import aws_s3tables_table_bucket.this arn:aws:s3tables:us-east-1:123456789012:bucket/amzn-s3tables-marketdata-bucket

# 3) import the namespace
import-namespace:
	terraform import awscc_s3tables_namespace.ns "arn:aws:s3tables:us-east-1:123456789012:bucket/amzn-s3tables-marketdata-bucket|marketdata"

# 4) import the table
import-table:
	terraform import awscc_s3tables_table.tbl "arn:aws:s3tables:us-east-1:123456789012:bucket/amzn-s3tables-marketdata-bucket|marketdata|s3_table"

# Replace just the table (keeps bucket/namespace)
replace-table:
	terraform apply -replace=awscc_s3tables_table.tbl

# Replace the namespace (will also force table recreation)
replace-namespace:
	terraform apply -replace=awscc_s3tables_namespace.ns

# Replace the bucket (destructive; name reuse may not be possible)
replace-bucket:
	terraform apply -replace=aws_s3tables_table_bucket.this

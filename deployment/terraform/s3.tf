# buckets
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "de-01-2023-q2-prj-code-"
  force_destroy = true
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = "de-01-2023-q2-prj-ingestion-"
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket_prefix = "de-01-2023-q2-prj-processed-"
  force_destroy = true
}

# lambda objects
resource "aws_s3_object" "ingestion_lambda" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = local.ingestion_package_name
  source = "${path.module}/zip/${local.ingestion_package_name}"
}

resource "aws_s3_object" "process_lambda" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = local.process_package_name
  source = "${path.module}/zip/${local.process_package_name}"
}

resource "aws_s3_object" "load_lambda" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = local.load_package_name
  source = "${path.module}/zip/${local.load_package_name}"
}

# buckets
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "de-01-2023-q2-prj-code-"
  force_destroy = true
}

# resource "aws_s3_bucket" "ingestion_bucket" {
#   bucket_prefix = "de-01-2023-q2-prj-ingestion-"
# }

resource "aws_s3_bucket" "processed_bucket" {
  bucket_prefix = "de-01-2023-q2-prj-processed-"
  force_destroy = true
}

# lambda objects
# resource "aws_s3_object" "ingestion_lambda" {
#   bucket = aws_s3_bucket.code_bucket.bucket
#   key    = "ingestion.zip"
#   source = "${path.module}/zip/ingestion.zip"
# }
# resource "aws_s3_object" "process_lambda" {
#   bucket = aws_s3_bucket.code_bucket.bucket
#   key    = "process.zip"
#   source = "${path.module}/zip/process.zip"
# }

resource "aws_s3_object" "load_lambda" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "load.zip"
  source = "${path.module}/zip/load.zip"
}

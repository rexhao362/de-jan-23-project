# create buckets

resource "aws_s3_bucket" "code_bucket" {
    bucket_prefix = "s3-de-code-query-queens-"
}
resource "aws_s3_bucket" "ingestion_bucket" {
    bucket_prefix = "s3-de-ingestion-query-queens-"
}
resource "aws_s3_bucket" "processed_bucket" {
    bucket_prefix = "s3-de-processed-query-queens-"
}

# create lambda-objects
resource "aws_s3_object" "ingestion_code" {
    bucket = aws_s3_bucket.code_bucket.bucket
    key = functions/ingestion/ingestion.zip
    source = "${path.module}/../function_zips/ingestion.zip"
}
resource "aws_s3_object" "process_code" {
    bucket = aws_s3_bucket.code_bucket.bucket
    key = functions/process/process.zip
    source = "${path.module}/../function_zips/process.zip"
}
resource "aws_s3_object" "load_code" {
    bucket = aws_s3_bucket.code_bucket.bucket
    key = functions/remodel/load.zip
    source = "${path.module}/../function_zips/load.zip"
}

# create bucket_notification / event bucket_notification
# Ingestion lambda triggered by eventbridge
# process lambda triggered by ingestion bucket event notification?
# load lambda triggered by process bucket event notification?

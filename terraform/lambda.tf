resource "aws_lambda_function" "ingestion_lambda" {
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = aws_s3_object.ingestion_lambda.key
    function_name = var.ingestion_lambda_name
    role = aws_iam_role.ingestion_lambda_role.arn
    handler = "ingestion.data_ingestion"
    runtime = "python3.9"

    source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256
}

resource "aws_lambda_function" "process_lambda" {
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = aws_s3_object.process_lambda.key
    function_name = var.process_lambda_name
    role = aws_iam_role.process_lambda_role.arn
    handler = "process.main_s3"
    runtime = "python3.9"

    source_code_hash = data.archive_file.process_lambda.output_base64sha256
}

resource "aws_lambda_function" "load_lambda" {
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = aws_s3_object.load_lambda.key
    function_name = var.load_lambda_name
    role = aws_iam_role.load_lambda_role.arn
    handler = "load_new_data_into_warehouse_db.load_new_data_into_warehouse_db"
    runtime = "python3.9"

    source_code_hash = data.archive_file.load_lambda.output_base64sha256
}
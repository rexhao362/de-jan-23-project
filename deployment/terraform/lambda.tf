resource "aws_lambda_function" "load_lambda" {
  s3_bucket     = aws_s3_bucket.code_bucket.bucket
  s3_key        = aws_s3_object.load_lambda.key
  function_name = local.load_lambda_name
  role          = aws_iam_role.load_lambda_execution_role.arn
  handler       = "processed_data_loader.${local.load_lambda_name}"
  runtime       = "python3.9"
  # from https://greeeg.com/en/issues/aws-lambda-ci-cd-pipeline-github-actions-terraform
  #   timeout = 1
  #   memory_size      = 128
  #   publish          = true

  source_code_hash = data.archive_file.load_lambda.output_base64sha256
}

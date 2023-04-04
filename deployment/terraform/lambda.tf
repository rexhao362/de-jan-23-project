resource "aws_lambda_function" "load_lambda" {
  s3_bucket     = aws_s3_bucket.code_bucket.bucket
  s3_key        = aws_s3_object.load_lambda.key
  function_name = local.load_lambda_name
  role          = aws_iam_role.load_lambda_execution_role.arn
  # handler       = "${local.load_module_name}.${local.load_lambda_name}" # TODO: use this!
  handler = "lambda_function.${local.load_lambda_name}"
  runtime = "python3.9"
  # AWSSDKPandas-Python39 x86_64
  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:5"]

  source_code_hash = data.archive_file.load_lambda.output_base64sha256
}

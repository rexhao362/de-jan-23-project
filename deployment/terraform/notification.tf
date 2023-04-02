# load_lambda

resource "aws_s3_bucket_notification" "processed_bucket" {
  bucket = aws_s3_bucket.processed_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    #filter_prefix       = "foldername"
    filter_suffix = "done.txt"
  }

  depends_on = [aws_lambda_permission.s3_permission_to_trigger_load_lambda]
}

resource "aws_lambda_permission" "s3_permission_to_trigger_load_lambda" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.processed_bucket.arn
}

# so far can change anything here without getting permission errors during Apply

# TODO: should be replaced to ingestion!
resource "aws_cloudwatch_event_rule" "ingestion_lambda_scheduled_run_rule" {
  name                = "run-ingestion-lambda-evary-five-minutes"
  description         = "Trigger ingestion lambda every 5 min"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  arn  = aws_lambda_function.ingestion_lambda.arn
  rule = aws_cloudwatch_event_rule.ingestion_lambda_scheduled_run_rule.name
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_ingestion_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_lambda_scheduled_run_rule.arn
}

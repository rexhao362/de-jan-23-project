# so far can change anything here without getting permission errors during Apply

# TODO: should be replaced to ingestion!
resource "aws_cloudwatch_event_rule" "ingestion_lambda_sheduled_run_rule" {
  name                = "run-ingestion-lambda-evary-two-minutes"
  description         = "Trigger ingestion lambda every 2 min"
  schedule_expression = "rate(2 minutes)"
}

resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  arn  = aws_lambda_function.load_lambda.arn # TODO: replace with ingestion_lambda
  rule = aws_cloudwatch_event_rule.ingestion_lambda_sheduled_run_rule.name
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_ingestion_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_lambda.function_name # TODO: replace with ingestion_lambda
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_lambda_sheduled_run_rule.arn
}

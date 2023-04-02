data "aws_caller_identity" "current" {}
data "aws_region" "current" {}


# Where we zip up python lambdas

# data "archive_file" "ingestion_lambda" {
#     type = "zip"
#     source_dir = "${path.module}/../src/lambdas/ingestion"
#     output_path = "${path.module}/../function_zips/ingestion.zip"
# }

# data "archive_file" "process_lambda" {
#     type = "zip"
#     source_dir = "${path.module}/../src/lambdas/process"
#     output_path = "${path.module}/../function_zips/process.zip"
# }

data "archive_file" "load_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../../alt_src/load"
  output_path = "${path.module}/zip/load.zip"
}



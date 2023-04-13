locals {
  region_name = "us-east-1"

  # ingestion
  ingestion_module_name  = "ingestion"
  ingestion_lambda_name  = "ingestion_lambda"
  ingestion_package_name = "ingestion_lambda.zip"

  # process
  process_module_name  = "process"
  process_lambda_name  = "main"
  process_package_name = "process_lambda.zip"

  # load
  load_module_name  = "processed_data_loader"
  load_lambda_name  = "load_processed_data"
  load_package_name = "load_lambda.zip"
}

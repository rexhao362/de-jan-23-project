# lambda get_table_names

variable "ingestion_lambda_name" {
    type = string
    default = "ingestion_lambda"
}
variable "process_lambda_name" {
    type = string
    default = "process_lambda"
}
variable "remodel_lambda_name" {
    type = string
    default = "remodel_lambda"
}
variable "region_name" {
    type = string
    default = "us-east-1"
}
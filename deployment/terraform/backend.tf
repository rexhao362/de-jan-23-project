terraform {
  backend "s3" {
    bucket  = "de-01-2023-q2-prj-terraform-backend-w3"
    key     = "state/terraform.tfstate"
    region  = "us-east-1"
    encrypt = "true"
  }
}

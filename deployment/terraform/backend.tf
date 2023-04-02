terraform {
  backend "s3" {
    bucket = "de-01-2023-q2-prj-terraform-backend"
    key    = "state/terraform.tfstate"
    region = "us-east-1"
  }
}

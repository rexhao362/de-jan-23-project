terraform {
    backend "s3" {
        bucket = "s3-de-backend-query-queens"
        key = "state/terraform.tfstate"
        region = "us-east-1"
    }
}

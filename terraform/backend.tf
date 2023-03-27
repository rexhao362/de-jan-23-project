terraform {
    backend "s3" {
        bucket = var.backend_bucket_name
        key = "state/terraform.tfstate"
        region = var.region_name
    }
}
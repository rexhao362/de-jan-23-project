#!/bin/bash

STRING="terraform {
    backend \"s3\" {
        bucket = \"s3-de-backend-query-queens\"
        key = \"state/terraform.tfstate\"
        region = \"us-east-1\"
    }
}"

touch terraform/backend.tf

echo "$STRING" > terraform/backend.tf

aws s3 mb s3://s3-de-backend-query-queens

echo "BACKEND BUCKET CREATED"
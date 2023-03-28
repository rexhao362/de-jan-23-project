# attach policies to role
# role attach to lambdas

# One role per lambda

resource "aws_iam_role" "ingestion_lambda_role" {
    name_prefix = "role-${var.ingestion_lambda_name}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role" "process_lambda_role" {
    name_prefix = "role-${var.process_lambda_name}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}


resource "aws_iam_role" "load_lambda_role" {
    name_prefix = "role-${var.load_lambda_name}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# one S3 policy for all lambdas

data "aws_iam_policy_document" "s3_document" {
    statement {

        actions = ["s3:GetObject", "s3:PutObject"]

        resources = [
        "${aws_s3_bucket.code_bucket.arn}/*",
        "${aws_s3_bucket.ingestion_bucket.arn}/*",
        "${aws_s3_bucket.process_bucket.arn}/*",
        ]
    }
}

# separate cw policies so we have log group for each lambda
# >>>>>>>>>>>>>>>>>>>>>>>>>

data "aws_iam_policy_document" "cw_document" {
    statement {

        actions = ["logs:CreateLogGroup"]

        resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]    
}

statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_lambda_name}:*", 
    ]
  }


}
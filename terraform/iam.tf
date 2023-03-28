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

# induvidual s3 policy documents for each lambda, to receive induvidual policies

data "aws_iam_policy_document" "ingestion_s3_document" {
    statement {

        actions = ["s3:GetObject", "s3:PutObject"]

        resources = [
        "${aws_s3_bucket.code_bucket.arn}/*",
        "${aws_s3_bucket.ingestion_bucket.arn}/*",
        "${aws_s3_bucket.process_bucket.arn}/*",
        ]
    }
}
data "aws_iam_policy_document" "process_s3_document" {
    statement {

        actions = ["s3:GetObject", "s3:PutObject"]

        resources = [
        "${aws_s3_bucket.code_bucket.arn}/*",
        "${aws_s3_bucket.ingestion_bucket.arn}/*",
        "${aws_s3_bucket.process_bucket.arn}/*",
        ]
    }
}
data "aws_iam_policy_document" "load_s3_document" {
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

data "aws_iam_policy_document" "ingestion_cw_document" {
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

data "aws_iam_policy_document" "process_cw_document" {
    statement {

        actions = ["logs:CreateLogGroup"]

        resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]    
}

statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.process_lambda_name}:*", 
    ]
  }
}

data "aws_iam_policy_document" "load_cw_document" {
    statement {

        actions = ["logs:CreateLogGroup"]

        resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]    
}

statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.load_lambda_name}:*", 
    ]
  }
}

# attach s3 policies to respective s3 policy documents

resource "aws_iam_policy" "ingestion_s3_policy" {
    name_prefix = "s3-policy-${var.ingestion_lambda_name}"
    policy = data.aws_iam_policy_document.ingestion_s3_document.json
}
resource "aws_iam_policy" "process_s3_policy" {
    name_prefix = "s3-policy-${var.process_lambda_name}"
    policy = data.aws_iam_policy_document.process_s3_document.json
}
resource "aws_iam_policy" "load_s3_policy" {
    name_prefix = "s3-policy-${var.load_lambda_name}"
    policy = data.aws_iam_policy_document.load_s3_document.json
}

# attach cw policies to respective cw policy documents

resource "aws_iam_policy" "ingestion_cw_policy" {
    name_prefix = "cw-policy-${var.ingestion_lambda_name}"
    policy = data.aws_iam_policy_document.ingestion_cw_document.json
}
resource "aws_iam_policy" "process_cw_policy" {
    name_prefix = "cw-policy-${var.process_lambda_name}"
    policy = data.aws_iam_policy_document.process_cw_document.json
}
resource "aws_iam_policy" "load_cw_policy" {
    name_prefix = "cw-policy-${var.load_lambda_name}"
    policy = data.aws_iam_policy_document.load_cw_document.json
}

# attatch s3 policy documents to each role

resource "aws_iam_role_policy_attachment" "ingestion_s3_policy_attachment" {
    role = aws_iam_role.ingestion_lambda_role.name
    policy_arn = aws_iam_policy.ingestion_s3_policy.arn
}
resource "aws_iam_role_policy_attachment" "process_s3_policy_attachment" {
    role = aws_iam_role.process_lambda_role.name
    policy_arn = aws_iam_policy.process_s3_policy.arn
}
resource "aws_iam_role_policy_attachment" "load_s3_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.load_s3_policy.arn
}

# attach cw policy documents to each role

resource "aws_iam_role_policy_attachment" "ingestion_s3_policy_attachment" {
    role = aws_iam_role.ingestion_lambda_role.name
    policy_arn = aws_iam_policy.ingestion_cw_policy.arn
}
resource "aws_iam_role_policy_attachment" "process_s3_policy_attachment" {
    role = aws_iam_role.process_lambda_role.name
    policy_arn = aws_iam_policy.process_cw_policy.arn
}
resource "aws_iam_role_policy_attachment" "load_s3_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.load_cw_policy.arn
}
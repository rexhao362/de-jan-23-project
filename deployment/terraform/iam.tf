## lambdas

# load
resource "aws_iam_role" "load_lambda_role" {
  name_prefix = "${local.load_lambda_name}-role-"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

data "aws_iam_policy_document" "load_lambda_s3_full_access_document" {
  statement {
    sid       = "1"
    actions   = ["s3:*", "s3-object-lambda:*"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "load_lambda_cw_document" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.load_lambda.function_name}:*",
    ]
  }
}

resource "aws_iam_policy" "load_lambda_s3_full_access_policy" {
  name_prefix = "${aws_lambda_function.load_lambda.function_name}-full-access-policy-"
  policy      = data.aws_iam_policy_document.load_lambda_s3_full_access_document.json
}

resource "aws_iam_policy" "load_lambda_cw_policy" {
  name_prefix = "cw-policy-${aws_lambda_function.load_lambda.function_name}"
  policy      = data.aws_iam_policy_document.load_lambda_cw_document.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_full_access_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_lambda_s3_full_access_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment_2" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_lambda_cw_policy.arn
}




























#### UNUSED/TO BE DELETED ####
## sheduler (now executes load_lambda, should be ingestion_lambda!)
# TODO: DELETE, NOT NEEDED

# DOESN'T WORK
resource "aws_iam_role" "load_lambda_execution_role" {
  name_prefix        = "role-execute-${local.load_lambda_name}"
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

data "aws_iam_policy_document" "load_lambda_s3_document" {
  statement {
    actions = ["s3:DeleteObject", "s3:GetObjectAttributes", "s3:ListBucket", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.processed_bucket.arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "load_lambda_s3_document_4" {
  statement {
    actions = ["s3:DeleteObject", "s3:GetObjectAttributes", "s3:ListBucket", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.processed_bucket.arn}/*"
    ]
  }

  statement {
    actions = ["s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.processed_bucket.arn}"
    ]
  }
}

resource "aws_iam_policy" "load_lambda_s3_policy" {
  name_prefix = "s3-policy-${aws_lambda_function.load_lambda.function_name}"
  policy      = data.aws_iam_policy_document.load_lambda_s3_document.json
}

resource "aws_iam_policy" "load_lambda_s3_policy_4" {
  name_prefix = "s3-policy-4-${aws_lambda_function.load_lambda.function_name}"
  policy      = data.aws_iam_policy_document.load_lambda_s3_document_4.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment_4" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_s3_policy_4.arn
}
resource "aws_iam_role" "sheduler_execution_role" {
  name_prefix        = "sheduler-${aws_lambda_function.load_lambda.function_name}"
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
                        "scheduler.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# TODO: DELETE, NOT NEEDED
data "aws_iam_policy_document" "sheduler_execution_document" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = ["*"]
  }
}

# TODO: DELETE, NOT NEEDED
resource "aws_iam_policy" "sheduler_execution_policy" {
  name_prefix = "sheduler-execution-policy-${aws_lambda_function.load_lambda.function_name}"
  policy      = data.aws_iam_policy_document.sheduler_execution_document.json
}

# TODO: DELETE, NOT NEEDED
resource "aws_iam_role_policy_attachment" "sheduler_execution_policy_attachment" {
  role       = aws_iam_role.sheduler_execution_role.name
  policy_arn = aws_iam_policy.sheduler_execution_policy.arn
}

# TODO: DELETE, NOT NEEDED
data "aws_iam_policy_document" "load_lambda_s3_document_2" {
  statement {
    actions = ["s3:DeleteObject", "s3:GetObjectAttributes", "s3:ListBucket", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.processed_bucket.arn}/*"
    ]
  }
}

# TODO: DELETE, NOT NEEDED
data "aws_iam_policy_document" "load_lambda_s3_document_3" {
  statement {
    actions = ["s3:*"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.processed_bucket.arn}/*"
    ]
  }
}

# TODO: DELETE, NOT NEEDED
resource "aws_iam_policy" "load_lambda_s3_policy_2" {
  name_prefix = "s3-policy-2-${aws_lambda_function.load_lambda.function_name}"
  policy      = data.aws_iam_policy_document.load_lambda_s3_document_2.json
}

# TODO: DELETE, NOT NEEDED
resource "aws_iam_policy" "load_lambda_s3_policy_3" {
  name_prefix = "s3-policy-3-${aws_lambda_function.load_lambda.function_name}"
  policy      = data.aws_iam_policy_document.load_lambda_s3_document_3.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment_2" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_s3_policy_2.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment_3" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_s3_policy_3.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_cw_policy.arn
}

# ## sheduler (now executes load_lambda, should be ingestion_lambda!)
# # based on https://docs.aws.amazon.com/scheduler/latest/UserGuide/setting-up.html#setting-up-execution-role
# # and https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/scheduler_schedule
# resource "aws_iam_role" "sheduler_execution_role" {
#   name_prefix        = "sheduler-${local.load_lambda_name}"
#   assume_role_policy = <<EOF
#     {
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Effect": "Allow",
#                 "Action": [
#                     "sts:AssumeRole"
#                 ],
#                 "Principal": {
#                     "Service": [
#                         "scheduler.amazonaws.com"
#                     ]
#                 }
#             }
#         ]
#     }
#     EOF
# }

# data "aws_iam_policy_document" "sheduler_execution_document" {
#   statement {
#     actions   = ["lambda:InvokeFunction"]
#     resources = ["*"]
#   }
# }

# resource "aws_iam_policy" "sheduler_execution_policy" {
#   name_prefix = "sheduler-execution-policy-${local.load_lambda_name}"
#   policy      = data.aws_iam_policy_document.sheduler_execution_document.json
# }

# resource "aws_iam_role_policy_attachment" "sheduler_execution_policy_attachment" {
#   role       = aws_iam_role.sheduler_execution_role.name
#   policy_arn = aws_iam_policy.sheduler_execution_policy.arn
# }

## lambdas

# load
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

data "aws_iam_policy_document" "load_lambda_cw_document" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.load_lambda_name}:*",
    ]
  }
}

resource "aws_iam_policy" "load_lambda_s3_policy" {
  name_prefix = "s3-policy-${local.load_lambda_name}"
  policy      = data.aws_iam_policy_document.load_lambda_s3_document.json
}

resource "aws_iam_policy" "load_lambda_cw_policy" {
  name_prefix = "cw-policy-${local.load_lambda_name}"
  policy      = data.aws_iam_policy_document.load_lambda_cw_document.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment" {
  role       = aws_iam_role.load_lambda_execution_role.name
  policy_arn = aws_iam_policy.load_lambda_cw_policy.arn
}

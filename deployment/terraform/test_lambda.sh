# from https://hands-on.cloud/terraform-lambda-tutorial/
aws lambda invoke \
  --function-name managing-alb-using-terraform-simple-lambda-lambda \
  --cli-binary-format raw-in-base64-out \
  /tmp/managing-alb-using-terraform-simple-lambda-lambda-response.json
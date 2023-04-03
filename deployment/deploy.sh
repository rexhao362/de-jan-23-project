aws configure
SOURCE_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
SUFFIX=$(date +%s)
PREFIX='test-q2'
DATA_BUCKET=${PREFIX}-processed-${SUFFIX}
CODE_BUCKET=${PREFIX}-code-${SUFFIX}
FUNCTION=load_processed_data

echo -n "Creating bucket ${CODE_BUCKET} .. "
aws s3 mb s3://${CODE_BUCKET}
echo "created"

echo -n "Creatin gbucket ${DATA_BUCKET} .. "
aws s3 mb s3://${DATA_BUCKET}
echo "created"

echo -n "Creating deployment package .. "
cd alt_src/load/
find ./ -type d -name '__pycache__' -exec rm -rf {} +
zip ../../deployment/terraform/zip/processed_data_loader.zip -r ./*
cd ../../
echo "created"

echo -n "Copying deployment package to ${CODE_BUCKET} bucket.. "
aws s3 cp ./deployment/zip/processed_data_loader.zip s3://${CODE_BUCKET}/processed_data_loader.zip
echo "copied"

# update policy with buckets' ARNs from the prev step first!
echo "Creating policy s3-read-policy .."
S3_READ_POLICY=$(aws iam create-policy --policy-name s3-read-policy --policy-document file://deployment/templates/s3_read_policy_template.json | jq .Policy.Arn | tr -d '"')
echo "policy ${S3_READ_POLICY} created"

# aws sts get-caller-identity | jq .Arn | tr -d '"'

echo "Creating policy clouwatch-log .."
CLOUDWATCH_POLICY=$(aws iam create-policy --policy-name clouwatch-log --policy-document file://deployment/templates/cloudwatch_log_policy_template.json | jq .Policy.Arn | tr -d '"')
echo "policy ${CLOUDWATCH_POLICY} created"

echo "Creating role execute-load .."
EXECUTION_ROLE=$(aws iam create-role --role-name execute-load --assume-role-policy-document file://deployment/trust_policy.json | jq .Role.Arn | tr -d '"')
echo "execution role ${EXECUTION_ROLE} created"

aws iam attach-role-policy --role-name execute-load --policy-arn ${S3_READ_POLICY}
aws iam attach-role-policy --role-name execute-load --policy-arn ${CLOUDWATCH_POLICY}

echo "Creating lambda function ${FUNCTION} .."
FUNCTION_ARN=$(aws lambda create-function --function-name ${FUNCTION} --runtime python3.9 --role ${EXECUTION_ROLE} --package-type Zip --handler processed_data_loader.load_processed_data --code S3Bucket=${CODE_BUCKET},S3Key=processed_data_loader.zip | jq .FunctionArn | tr -d '"')
echo "lambda function ${FUNCTION_ARN} created"

aws lambda add-permission --function-name ${FUNCTION} --statement-id invoke-load --action "lambda:InvokeFunction" --principal s3.amazonaws.com --source-arn arn:aws:s3:::${DATA_BUCKET} --source-account ${SOURCE_ACCOUNT}

aws s3api put-bucket-notification-configuration --bucket ${DATA_BUCKET} --notification-configuration file://deployment/templates/s3_event_config_template.json 

# echo "Test" > test.txt
# aws s3 cp test.txt s3://${DATA_BUCKET}/test.txt
aws logs tail /aws/lambda/${FUNCTION}






 2002  aws configure
 2007  SUFFIX=$(date +%s)
 PREFIX='test-q2'
 2010  DATA_BUCKET=${PREFIX}-processed-${SUFFIX}
 2011  CODE_BUCKET=${PREFIX}-code-${SUFFIX}
 2012  FUNCTION=load_processed_data

 2014  zip function.zip src/file_reader/reader.py 
 
 find ./ -type d -name '__pycache__' -exec rm -rf {} +
 zip ../processed_data_loader.zip -r ./*

 2016  aws s3 mb s3://${CODE_BUCKET}
 2017  aws s3 mb s3://${DATA_BUCKET}
 2018  aws s3 cp function.zip s3://${CODE_BUCKET}/${FUNCTION}/function.zip


 aws s3 cp ../processed_data_loader.zip s3://${CODE_BUCKET}/processed_data_loader.zip

 aws s3 ls --recursive s3://${CODE_BUCKET}

 2027  RES=$(aws iam create-policy --policy-name s3-read --policy-document deployment/templates/s3_read_policy_template.json )
 2028  RES=$(aws iam create-policy --policy-name s3-read --policy-document file://deployment/templates/s3_read_policy_template.json )
 2030  echo $RES | jq .Policy.Arn
 2032  S3_READ_POLICY=$(echo $RES | jq .Policy.Arn | tr -d '"')
 2037  aws sts get-caller-identity | jq .Arn | tr -d '"'
 2039  CLOUDWATCH_POLICY=$(aws iam create-policy --policy-name clouwatch-log --policy-document file://deployment/templates/cloudwatch_log_policy_template.json) | jq .Policy.Arn | tr -d '"'
 2041  aws iam create-policy --policy-name clouwatch-log --policy-document file://deployment/templates/cloudwatch_log_policy_template.json)
 2042  aws iam create-policy --policy-name clouwatch-log --policy-document file://deployment/templates/cloudwatch_log_policy_template.json
 2043  CLOUDWATCH_POLICY=arn:aws:iam::287342867198:policy/clouwatch-log
 2044  EXECUTION_ROLE=$(aws iam create-role --role-name execute-read-s3-file --assume-role-policy-document file://deployment/trust_policy.json | jq .Role.Arn | tr -d '"')
 
 2049  aws iam attach-role-policy --role-name execute-read-s3-file --policy-arn ${S3_READ_POLICY}
 2050  aws iam attach-role-policy --role-name execute-read-s3-file --policy-arn ${CLOUDWATCH_POLICY}
 2051  aws lambda create-function --function-name ${FUNCTION} --runtime python3.9 --role ${EXECUTION_ROLE} --package-type Zip --handler reader.lambda_handler --code S3Bucket=${CODE_BUCKET},S3Key=${FUNCTION}/function.zip
 2052  aws lambda add-permission --function-name ${FUNCTION} --statement-id invoke-read-s3-file --action "lambda:InvokeFunction" --principal s3.amazonaws.com --source-arn arn:aws:s3:::${DATA_BUCKET} --source-account 287342867198
 2061  FUNCTION_ARN=$(aws lambda list-functions | jq .Functions[0].FunctionArn | tr -d '"')
 2062  aws s3api put-bucket-notification-configuration --bucket ${DATA_BUCKET} --notification-configuration file://deployment/templates/s3_event_config_template.json 
 2064  echo "Test" > test.txt
 2065  aws s3 cp test.txt s3://${DATA_BUCKET}/test.txt
 2066  aws logs tail /aws/lambda/read-s3-file


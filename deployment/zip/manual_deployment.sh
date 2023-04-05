find ./ -type d -name '__pycache__' -exec rm -rf {} +
rm -rf pandas* numpy*
rm -rf cramjam* yarl* frozenlist* multidict*
zip -9 -r ../zip/lambda.zip .
cd ../zip/
aws s3 rm s3://de-01-2023-q2-prj-code-20230403095204589100000005/lambda.zip
aws s3 cp lambda.zip s3://de-01-2023-q2-prj-code-20230403095204589100000005/lambda.zip

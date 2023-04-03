# EventBridge sheduler
RULE_ARN=$(aws events put-rule --name "EveryMinuteEvent" --schedule-expression "rate(1 minute)")
LAMBDA_ARN=arn:aws:lambda:us-east-1:168834843843:function:test_eb
aws lambda add-permission --function-name $LAMBDA_ARN --action lambda:InvokeFunction --statement-id events --principal events.amazonaws.com
# Result: {
#     "Statement": "{\"Sid\":\"events\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"events.amazonaws.com\"},\"Action\":\"lambda:InvokeFunction\",\"Resource\":\"arn:aws:lambda:us-east-1:168834843843:function:test_eb\"}"
# }
aws events put-targets --rule EveryMinuteEvent --targets "Id"="1","Arn"="$LAMBDA_ARN"
# Result: {
#     "FailedEntryCount": 0,
#     "FailedEntries": []
# }

to pause:
aws events disable-rule --name "EveryMinuteEvent"
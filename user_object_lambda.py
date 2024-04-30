import boto3
import pandas as pd
s3 = boto3.client('s3')

print('Original object from the S3 bucket:')
original = s3.get_object(
  Bucket='datacartconnectraw',
  Key='user_daily_data.csv')

data_str = original['Body'].read().decode('utf-8')
print(data_str)

print('Object processed by S3 Object Lambda:')
transformed = s3.get_object(
  Bucket='arn:aws:s3-object-lambda:eu-north-1:975050311718:accesspoint/mail',
  Key='user_daily_data.csv')
data_str = transformed['Body'].read().decode('utf-8')
print(data_str)
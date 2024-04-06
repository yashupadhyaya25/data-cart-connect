import json
import requests as rq
from datetime import datetime as dt
from datetime import timedelta as td
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    s3_bucket = event['bucket']
    folder = event['folder']
    try :
        date = event['date']
    except :
        date = dt.strftime(dt.now(),'%Y-%m-%d')
    response = get_users(s3_bucket,folder,date)
    return response

def get_users(s3_bucket,folder,date):
    try :
        api_url = 'https://fakestoreapi.com/users'
        response = rq.get(api_url)
        if response.status_code != 200 :
            return {'statusCode': str(response.status_code),'Message' : 'API encountered an error'}
        json_data = str(response.json()).replace("'",'"')
        s3_file_name = folder+'user_daily_data_'+date.replace('-','')+'.json'
        s3_client.put_object(Body = json_data,Key = s3_file_name,Bucket = s3_bucket)
        return {
        'statusCode': 200,
        'body': json.dumps('Data Fetch Successfully')
        }
    except Exception as e:
        return {'statusCode': 400,'Message' : str(e)}
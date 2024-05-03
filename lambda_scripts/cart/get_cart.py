import json
import requests as rq
from datetime import datetime as dt
from datetime import timedelta as td
import boto3
import random
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    s3_bucket = os.environ['bucket']
    folder = os.environ['folder']
    try :
        date = event['date']
    except :
        date = dt.strftime(dt.now(),'%Y-%m-%d')
    response = daily_cart_data(s3_bucket,folder,date)
    return response

def daily_cart_data(s3_bucket,folder,date):
    try :
        # new lambda
        file_date = date
        api_url = 'https://fakestoreapi.com/carts?startdate='+date+'&enddate='+date
        response =rq.get(api_url)
        if response.json() == [] :
            date_list = ['2020-03-01','2020-03-02','2020-01-02','2020-01-01']
            date = random.choice(date_list)
        api_url = 'https://fakestoreapi.com/carts?startdate='+date+'&enddate='+date
        response =rq.get(api_url)
        
        if response.status_code != 200 :
            return {'statusCode': str(response.status_code),'Message' : 'API encountered an error'}
        
        json_data = str(response.json()).replace("'",'"')
        s3_file_name = folder+'cart_daily_data_'+file_date.replace('-','')+'.json'
        s3_client.put_object(Body = json_data,Key = s3_file_name,Bucket = s3_bucket)
        return {
        'statusCode': 200,
        'body': json.dumps('Data Fetch Successfully')
        }
    except Exception as e:
        return {'statusCode': 400,'Message' : str(e)}
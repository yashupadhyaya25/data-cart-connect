import json
from datetime import datetime as dt
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['bucket']
    folder_path = event['folder']
    file_flag = event['file_flag']
    date_flag = event['date_flag']
    if date_flag :
        try :
            date = event['date']
        except :
            date = dt.strftime(dt.now(),'%Y-%m-%d')
    else :
        date = ''

    if file_flag :    
        file_name = event['file_name']
        file_extension = event['file_extension']
        file_name = file_name + '_' + date.replace('-','') + file_extension
        response = file_exists(bucket,folder_path,file_name)
    else :
        file_name = event['file_name']
        if date != '' :
            file_name = file_name + '_' + date.replace('-','')
        response = folder_exists(bucket,folder_path,file_name)
    return response
    
def file_exists(bucket,folder_path,file_name) :
    bucket = bucket.lower()
    folder = folder_path.lower()
    file_name = file_name.lower()
    try :
        res = s3_client.get_object_attributes(Bucket = bucket,Key = folder + file_name,ObjectAttributes=['Checksum','ObjectParts','StorageClass','ObjectSize'])
        return {'StatusCode' : 200,'Message' : 'File Exists'}
    except :
        return {'StatusCode' : 400, 'Message' : 'File Does Not Exists'}

def folder_exists(bucket,folder_path,file_name) :
    bucket = bucket.lower()
    folder = folder_path.lower()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder_path+file_name+'/')
    print(response)
    if response.get('KeyCount') > 0 or response.get('KeyCount') == None:
        return {'StatusCode' : 200,'Message' : 'File Exists'}
    return {'StatusCode' : 400, 'Message' : 'File Does Not Exists'}
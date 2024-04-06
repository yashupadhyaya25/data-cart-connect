from pyspark.sql import SparkSession
import boto3
from pyspark.sql.functions import col
import json
import sys
from datetime import datetime as dt

spark= SparkSession \
       .builder \
       .appName("user") \
       .getOrCreate()

json_agrs = sys.argv
args_json = json.loads(json_agrs[-1].replace("'",'"'))
secret_name = args_json.get('SecretName')
region_name = args_json.get('RegionName')

spark = SparkSession.builder.getOrCreate()
session = boto3.session.Session()

client = session.client(service_name='secretsmanager', region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
get_secret_value_response = json.loads(get_secret_value_response['SecretString'])
raw_bucket = get_secret_value_response['raw_bucket']
bronze_bucket = get_secret_value_response['bronze_bucket']
raw_input_folder = get_secret_value_response['user_raw_input_folder']
raw_archive_folder = get_secret_value_response['user_raw_archive_folder']
raw_issue_folder = get_secret_value_response['user_raw_issue_folder']
bronze_input_folder = get_secret_value_response['user_bronze_input_folder']
s3_client = boto3.client('s3')
file_list_in_raw = s3_client.list_objects_v2(Bucket=raw_bucket, Prefix=raw_input_folder, Delimiter='/')

def file_validation_check(df):
  ## Column Validation
  mandatory_col = df.columns
  address_flag =  'address' in mandatory_col
  email_flag = 'email' in mandatory_col
  id_flag = 'id' in mandatory_col
  name_flag = 'name' in mandatory_col
  password_flag = 'password' in mandatory_col
  phone_flag = 'phone' in mandatory_col
  username_flag = 'username' in mandatory_col
  return  (address_flag and email_flag and id_flag and name_flag and password_flag and phone_flag and username_flag)

def main(date) :
  raw_file_name = 'user_daily_data_'+date.replace('-','')+'.json'
  try :
    user_df = spark.read.json('s3://'+raw_bucket+'/'+raw_input_folder+raw_file_name)
    validation_flag = file_validation_check(user_df)
    if validation_flag :
      user_df = user_df.select(
          col('id').alias('ID'),
          col('username').alias('USER_NAME'),
          col('email').alias('EMAIL_ID'),
          col('name.firstname').alias('FIRST_NAME'),
          col('name.lastname').alias('LAST_NAME'),
          col('password').alias('PASSWORD'),
          col('phone').alias('PHONE'),
          col('address.city').alias('CITY'),
          col('address.number').alias('HOUSE_NUMBER'),
          col('address.zipcode').alias('ZIP_CODE'),
          col('address.street').alias('STREET'),
          col('address.geolocation.lat').alias('LATITUDE'),
          col('address.geolocation.long').alias('LONGITUDE')
      )
      user_df.coalesce(1).write.mode('overwrite').option("header","true").csv('s3://'+bronze_bucket+'/'+bronze_input_folder+raw_file_name.rsplit('.',-1)[0])
      # shutil.move(raw_input_folder+raw_file_name,raw_archive_folder+raw_file_name)
      s3_client.copy_object(Bucket=raw_bucket,Key=raw_archive_folder+raw_file_name,
                            CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
      s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
      return {'StatusCode' : 200,'Message':'Raw To Bronze Data Load Successfully'}
    else :
      # shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
      s3_client.copy_object(Bucket=raw_bucket,Key=raw_issue_folder+raw_file_name,
                            CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
      s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
      return {'StatusCode' : 400,'Message':'Missing Required Column'}
  except Exception as e:
    # shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
    s3_client.copy_object(Bucket=raw_bucket,Key=raw_issue_folder+raw_file_name,
                          CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
    s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
    return {'StatusCode' : 400,'Message':str(e)}

if __name__ == "__main__" :
    json_agrs = sys.argv
    args_json = json.loads(json_agrs[-1].replace("'",'"'))
    try :
      date = args_json.get('Date')
      res = main(date)
    except :
      res = main(dt.strftime(dt.now(),'%Y-%m-%d'))

import json
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import shutil
import boto3
import sys
from datetime import datetime as dt

spark = SparkSession.builder.getOrCreate()
session = boto3.session.Session()

json_agrs = sys.argv
args_json = json.loads(json_agrs[-1].replace("'",'"'))
secret_name = args_json.get('SecretName')
region_name = args_json.get('RegionName')

client = session.client(service_name='secretsmanager', region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
raw_bucket = json.loads(get_secret_value_response['SecretString'])['raw_bucket']
bronze_bucket = json.loads(get_secret_value_response['SecretString'])['bronze_bucket']
raw_input_folder =  json.loads(get_secret_value_response['SecretString'])['cart_raw_input_folder']
raw_archive_folder = json.loads(get_secret_value_response['SecretString'])['cart_raw_archive_folder']
raw_issue_folder = json.loads(get_secret_value_response['SecretString'])['cart_raw_issue_folder']
raw_bronze_folder = json.loads(get_secret_value_response['SecretString'])['cart_bronze_input_folder']
s3_client = boto3.client('s3')
file_list_in_raw = s3_client.list_objects_v2(Bucket=raw_bucket, Prefix=raw_input_folder, Delimiter='/')

def data_validation(col_names):
      date_flag = 'date' in col_names
      id_flag = 'id' in col_names
      products_flag = 'products' in col_names
      userId_flag = 'userId' in col_names

      return (date_flag and id_flag and products_flag and userId_flag)

def get_cart_details(date):

    raw_file_name = "cart_daily_data_"+date.replace("-","")+".json"

    try:
      df = spark.read.json('s3://'+raw_bucket+'/'+raw_input_folder+raw_file_name)
      # df.show()
      df = df.drop(df['date'])
      df = df.withColumn('date',F.lit(date))

      col_names = df.columns
      print('STEP - 1')
      if(data_validation(col_names)):
        df = df.select(df['date'].alias('DATE'),df['id'].alias('ID'),F.explode(df['products']).alias('products'),df['userId'].alias('USER_ID'))
        df = df.select('DATE','ID','USER_ID',df['products.productId'].alias('PRODUCT_ID'),df['products.quantity'].alias('PRODUCT_QUANTITY'))
        # df.show()
        print('STEP - 2')
        df.write.option("header",True).mode("overwrite").parquet('s3://'+bronze_bucket+'/'+raw_bronze_folder+raw_file_name.split('.')[0])
        print('STEP - 3')
        #Run successful-> put input file to archive
        # shutil.move(raw_input_folder+raw_file_name,raw_archive_folder+raw_file_name)
        s3_client.copy_object(Bucket=raw_bucket,Key=raw_archive_folder+raw_file_name,
                              CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
        s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
        print('STEP - 4')
        return {'statusCode': 200,'Message' : 'Data Load Successfully in Bronze Layer'}
      else:
        # shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
        print('STEP - 5')
        s3_client.copy_object(Bucket=raw_bucket,Key=raw_issue_folder+raw_file_name,
                              CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
        s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
        print('STEP - 6')
        return {'statusCode' : 400, 'Message': 'Required Columns are missing'}

    except Exception as e:
      print('STEP - 7')
      #there is some issue in file -> move input file to issue folder
      # shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
      s3_client.copy_object(Bucket=raw_bucket,Key=raw_issue_folder+raw_file_name,
                            CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
      s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
      print(e)
      print('STEP - 8')
      return {'statusCode': 400,'Message' : str(e)}


if __name__ == '__main__' :
    json_agrs = sys.argv
    args_json = json.loads(json_agrs[-1].replace("'",'"'))
    try :
      date = args_json.get('Date')
      res = get_cart_details(date)
    except :
      res = get_cart_details(dt.strftime(dt.now(),'%Y-%m-%d'))



from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
import json
import sys 
from datetime import datetime as dt

spark = SparkSession \
       .builder \
       .appName("product") \
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
raw_input_folder = get_secret_value_response['product_raw_input_folder']
raw_archive_folder = get_secret_value_response['product_raw_archive_folder']
raw_issue_folder = get_secret_value_response['product_raw_issue_folder']
bronze_input_folder = get_secret_value_response['product_bronze_input_folder']

s3_client = boto3.client('s3')
file_list_in_raw = s3_client.list_objects_v2(Bucket=raw_bucket, Prefix=raw_input_folder, Delimiter='/')

def column_validation(df):
  try :
    columns = [x.lower() for x in df.columns]
    id_flag = 'id' in columns
    category_flag = 'category' in columns
    description_flag = 'description' in columns
    image_flag = 'image' in columns
    price_flag = 'price' in columns
    rating_flag = 'rating' in columns
    title_flag = 'title' in columns
    return (id_flag and category_flag and description_flag and image_flag and price_flag and rating_flag and title_flag)
  except :
    return False

def main(date) :
  raw_file_name = 'product_daily_data_'+date.replace('-','')+'.json'
  try :
    product_df = spark.read.json('s3://'+raw_bucket+'/'+raw_input_folder+raw_file_name)
    validation_flag = column_validation(product_df)
    if validation_flag :
      product_df = product_df.select(
          col('id').alias('ID'),
          col('title').alias('TITLE'),
          col('category').alias('CATEGORY'),
          col('description').alias('DESCRIPTION'),
          col('price').alias('PRICE'),
          col('rating.count').alias('RATING_COUNT'),
          col('rating.rate').alias('RATING_RATE'),
          col('image').alias('IMAGE_URL')
      )
      product_df.write.option('header',True).mode('overwrite').csv('s3://'+bronze_bucket+'/'+bronze_input_folder+raw_file_name.rsplit('.',-1)[0])
      # shutil.move(raw_input_folder+raw_file_name,raw_archive_folder+raw_file_name)
      s3_client.copy_object(Bucket=raw_bucket,Key=raw_archive_folder+raw_file_name,
                            CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
      s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
      return {'StatusCode' : 200,'Message' : 'Data Loaded Succesfully From Raw To Bronze'}
    else :
      # shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
      s3_client.copy_object(Bucket=raw_bucket,Key=raw_issue_folder+raw_file_name,
                            CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
      s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
      return {'StatusCode' : 400,'Message' : 'Mandatory Columns Are Missing'}
  except Exception as e:
    # shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
    s3_client.copy_object(Bucket=raw_bucket,Key=raw_issue_folder+raw_file_name,
                          CopySource=raw_bucket+'/'+raw_input_folder+raw_file_name)
    s3_client.delete_object(Bucket=raw_bucket,Key=raw_input_folder+raw_file_name)
    return {'StatusCode' : 400,'Message' : 'File Not Exists or '+str(e)}

if __name__ == "__main__" :
    json_agrs = sys.argv
    args_json = json.loads(json_agrs[-1].replace("'",'"'))
    try :
      date = args_json.get('Date')
      res = main(date)
    except :
      res = main(dt.strftime(dt.now(),'%Y-%m-%d'))
    
     



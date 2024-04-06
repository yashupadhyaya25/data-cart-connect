from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime as dt
from datetime import timedelta as td
from pyspark.sql.types import StructType
import boto3,json
import sys 

spark = SparkSession.builder.appName("bronze_to_silver_cart").getOrCreate()
session = boto3.session.Session()

json_agrs = sys.argv
args_json = json.loads(json_agrs[-1].replace("'",'"'))
secret_name = args_json.get('SecretName')
region_name = args_json.get('RegionName')
client = session.client(service_name='secretsmanager', region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
get_secret_value_response = json.loads(get_secret_value_response['SecretString'])
silver_bucket = get_secret_value_response['silver_bucket']
bronze_bucket = get_secret_value_response['bronze_bucket']
bronze_input_folder = get_secret_value_response['cart_bronze_input_folder']
bronze_archive_folder = get_secret_value_response['cart_bronze_input_folder']
bronze_issue_folder = get_secret_value_response['cart_bronze_input_folder']
bronze_silver_folder = get_secret_value_response['cart_silver_folder']
product_bronze_silver_folder = get_secret_value_response['product_silver_folder']
s3_client = boto3.client('s3')

def file_exists_check(file_list) :
  try :
    for file in file_list.get('Contents') :
      if '.parquet' in file.get('Key').lower() :
        print('File Found')
        return True
  except :
    print('File Not Found')
    return False

def get_cart_details(date):
    bronze_file_name = "cart_daily_data_"+date.replace("-","")
    # print(bronze_file_name)

    try:
      year = date.split("-")[0]
      month = date.split("-")[1]
      day = date.split("-")[2]
      file_list_in_bronze = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_input_folder+bronze_file_name+'/', Delimiter='/')
      if file_exists_check(file_list_in_bronze):

        df = spark.read.parquet('s3://'+bronze_bucket+'/'+bronze_input_folder+bronze_file_name)
        product_silver_df = spark.read.parquet('s3://'+silver_bucket+'/'+product_bronze_silver_folder).filter((F.col('START_DATE') <= date) & (F.col('END_DATE') > date))
        final_df = product_silver_df.alias('product').join(df.alias('cart')\
                                    ,F.col('product.ID') == F.col('cart.PRODUCT_ID')\
                                    ,'inner')
        # final_df.show()
        final_df = final_df.select('product.PRODUCT_KEY'
                        ,'cart.DATE'
                        ,'cart.ID'
                        ,'cart.USER_ID'
                        ,'cart.PRODUCT_ID'
                        ,'cart.PRODUCT_QUANTITY')


        final_df = final_df.withColumn('YEAR',F.lit(year)).withColumn('MONTH',F.lit(month)).withColumn('DAY',F.lit(day)).withColumn('INSERTED_ON',F.lit(dt.now()))

        try :
          partition_path = 's3://'+silver_bucket+"/"+bronze_silver_folder+'YEAR='+year+'/MONTH='+month+'/DAY='+day
          existing_partitions = spark.read.parquet(partition_path)
        except :
          emptRDD = spark.sparkContext.emptyRDD()
          schema = StructType([])
          existing_partitions = spark.createDataFrame(emptRDD,schema)

        if existing_partitions.count() > 0:
          file_list_in_silver = s3_client.list_objects_v2(Bucket=silver_bucket, 
                                                          Prefix=bronze_silver_folder+'YEAR='+year+'/MONTH='+month+'/DAY='+day+'/', 
                                                          Delimiter='/')
          for file in file_list_in_silver.get('Contents') :
            file = file.get('Key')
            s3_client.delete_object(Bucket=silver_bucket,Key=file)
            print(file+' deleted')

        final_df.write.option("header",True).mode("append")\
        .partitionBy("YEAR","MONTH","DAY")\
        .parquet('s3://'+silver_bucket+'/'+bronze_silver_folder)
        # shutil.move(bronze_input_folder+bronze_file_name,bronze_archive_folder+bronze_file_name)
            # Archive File In Bronze
        file_list_in_bronze = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_input_folder+bronze_file_name+'/', Delimiter='/')
        for file in file_list_in_bronze.get('Contents') :
          file = file.get('Key')
          s3_client.copy_object(Bucket=bronze_bucket,Key=file.replace('input','archive'),
                                CopySource=bronze_bucket+'/'+file)
          s3_client.delete_object(Bucket=bronze_bucket,Key=file)
        print('Data Load Successfully in Silver Layer')
        return {'statusCode': 200,'Message' : 'Data Load Successfully in Silver Layer'}
      else:
         print('File Does not exist in Bronze Input Layer.')
         return {'statusCode': 400,'Message' : 'File Does not exist in Bronze Input Layer.'}
    except Exception as e:
    #   shutil.move(bronze_input_folder+bronze_file_name,bronze_issue_folder+bronze_file_name)
      file_list_in_bronze = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_input_folder+bronze_file_name+'/', Delimiter='/')
      for file in file_list_in_bronze.get('Contents') :
        file = file.get('Key')
        s3_client.copy_object(Bucket=bronze_bucket,Key=file.replace('input','issue'),
                              CopySource=bronze_bucket+'/'+file)
        s3_client.delete_object(Bucket=bronze_bucket,Key=file)
      print(e)
      return {'statusCode': 400,'Message' : str(e)}


if __name__ == '__main__' :
    try :
      json_agrs = sys.argv
      date_json = json.loads(json_agrs[-1].replace("'",'"'))
      date = date_json.get('Date')
      res = get_cart_details(date)
    except :
      res = get_cart_details(dt.strftime(dt.now(),'%Y-%m-%d'))
    print(res)


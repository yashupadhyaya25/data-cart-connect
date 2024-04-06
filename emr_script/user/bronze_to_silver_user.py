import pyspark
from pyspark.sql import DataFrame, SparkSession
from typing import List
import pyspark.sql.types as T
import pyspark.sql.functions as F
import boto3
from datetime import datetime as dt
from pyspark.sql.functions import col
from pyspark.sql.types import *
import json
import sys

spark = SparkSession \
       .builder \
       .appName("user") \
       .getOrCreate()

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
bronze_input_folder = get_secret_value_response['user_bronze_input_folder']
bronze_archive_folder = get_secret_value_response['user_bronze_input_folder']
bronze_issue_folder = get_secret_value_response['user_bronze_input_folder']
silver_input_folder = get_secret_value_response['user_silver_folder']
s3_client = boto3.client('s3')


def file_exists_check(file_list) :
  print(file_list)
  try :
    for file in file_list.get('Contents') :
      if '.csv' in file.get('Key').lower() :
        return True
  except :
    return False

def main(date) :
  bronze_file_name = 'user_daily_data_'+date.replace('-','')
  file_list_in_bronze = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_input_folder+bronze_file_name+'/', Delimiter='/')
  try :
    if file_exists_check(file_list_in_bronze) :
      try :
        silver_user_df = spark.read.option('header',True).parquet("s3://"+silver_bucket+'/'+silver_input_folder)
        silver_user_df = silver_user_df.filter(silver_user_df['START_DATE'] != date)

      except :
        emp_RDD = spark.sparkContext.emptyRDD()
        columns = StructType([])
        silver_user_df = spark.createDataFrame(data = emp_RDD,schema=columns)
      user_df = spark.read.option('header',True).csv("s3://"+bronze_bucket+'/'+bronze_input_folder+bronze_file_name)
      if silver_user_df.count() == 0 :
          user_df = user_df.filter(user_df['ID'] <= 8)
          user_df = user_df.withColumn('PASSWORD',
                                    F.when(user_df['USER_NAME'].isin('johnd','donero'),'TEST@1234')\
                                    .otherwise(col('PASSWORD'))
                                    )
          #add start_date,end_date and active_record
          user_df = user_df\
                        .withColumn('START_DATE',F.lit(date))\
                        .withColumn('END_DATE',F.lit('9999-12-31'))\
                        .withColumn('ACTIVE_RECORD',F.lit(1))

          #add Surrogate Key to user_df
          user_df = user_df.withColumn('USER_KEY',F.lit(F.md5(F.concat(user_df.ID,F.lit('_'),user_df.START_DATE))))
          user_df.show()
          user_df.coalesce(1).write.mode('overwrite').parquet("s3://"+silver_bucket+'/'+silver_input_folder)
      else:
          new_user_df = user_df.join(silver_user_df,on='ID', how='left_anti')
          new_user_df = new_user_df\
                        .withColumn('START_DATE',F.lit(date))\
                        .withColumn('END_DATE',F.lit('9999-12-31'))\
                        .withColumn('ACTIVE_RECORD',F.lit(1))

          user_df = user_df.withColumn('START_DATE',F.lit(date))\
                        .withColumn('END_DATE',F.lit('9999-12-31'))\

          # add prefix to dertermine old new data and which data needs/need_not to update
          prefix = "new_"
          for column in user_df.columns:
            user_df = user_df.withColumnRenamed(column,prefix + column)

          prefix = "old_"
          for column in silver_user_df.columns:
            silver_user_df = silver_user_df.withColumnRenamed(column,prefix + column)


          updated_user_df = user_df.alias('new_data').join(silver_user_df.alias('old_data')\
                            ,col('new_data.new_ID') == col('old_data.old_ID')\
                            ,'inner')\
                            .where(col('new_data.new_START_DATE') <= col('old_data.old_END_DATE'))

          no_need_to_updated_user_df = user_df.alias('new_data').join(silver_user_df.alias('old_data')
                      ,col('new_data.new_ID') == col('old_data.old_ID')
                      ,'inner')\
                      .where(col('new_data.new_START_DATE') > col('old_data.old_END_DATE'))

          no_need_to_updated_user_df = no_need_to_updated_user_df.select(
              col('old_ID').alias('ID'),
              col('old_USER_NAME').alias('USER_NAME'),
              col('old_EMAIL_ID').alias('EMAIL_ID'),
              col('old_FIRST_NAME').alias('FIRST_NAME'),
              col('old_LAST_NAME').alias('LAST_NAME'),
              col('old_PASSWORD').alias('PASSWORD'),
              col('old_PHONE').alias('PHONE'),
              col('old_CITY').alias('CITY'),
              col('old_HOUSE_NUMBER').alias('HOUSE_NUMBER'),
              col('old_ZIP_CODE').alias('ZIP_CODE'),
              col('old_STREET').alias('STREET'),
              col('old_LATITUDE').alias('LATITUDE'),
              col('old_LONGITUDE').alias('LONGITUDE'),
              col('old_START_DATE').alias('START_DATE'),
              col('old_END_DATE').alias('END_DATE'),
              col('old_ACTIVE_RECORD').alias('ACTIVE_RECORD')
              )

          updated_user_df_active = updated_user_df.select(
              col('new_ID').alias('ID'),
              col('new_USER_NAME').alias('USER_NAME'),
              col('new_EMAIL_ID').alias('EMAIL_ID'),
              col('new_FIRST_NAME').alias('FIRST_NAME'),
              col('new_LAST_NAME').alias('LAST_NAME'),
              col('new_PASSWORD').alias('PASSWORD'),
              col('new_PHONE').alias('PHONE'),
              col('new_CITY').alias('CITY'),
              col('new_HOUSE_NUMBER').alias('HOUSE_NUMBER'),
              col('new_ZIP_CODE').alias('ZIP_CODE'),
              col('new_STREET').alias('STREET'),
              col('new_LATITUDE').alias('LATITUDE'),
              col('new_LONGITUDE').alias('LONGITUDE'),
              col('new_START_DATE').alias('START_DATE'),
              col('new_END_DATE').alias('END_DATE'),
              F.lit(1).alias('ACTIVE_RECORD')
          )

          updated_user_df_inactive = updated_user_df.select(
              col('old_ID').alias('ID'),
              col('old_USER_NAME').alias('USER_NAME'),
              col('old_EMAIL_ID').alias('EMAIL_ID'),
              col('old_FIRST_NAME').alias('FIRST_NAME'),
              col('old_LAST_NAME').alias('LAST_NAME'),
              col('old_PASSWORD').alias('PASSWORD'),
              col('old_PHONE').alias('PHONE'),
              col('old_CITY').alias('CITY'),
              col('old_HOUSE_NUMBER').alias('HOUSE_NUMBER'),
              col('old_ZIP_CODE').alias('ZIP_CODE'),
              col('old_STREET').alias('STREET'),
              col('old_LATITUDE').alias('LATITUDE'),
              col('old_LONGITUDE').alias('LONGITUDE'),
              col('old_START_DATE').alias('START_DATE'),
              col('new_START_DATE').alias('END_DATE'),
              F.lit(0).alias('ACTIVE_RECORD')
          )

          final_user_df = new_user_df\
                            .union(updated_user_df_active)\
                            .union(updated_user_df_inactive)\
                            .union(no_need_to_updated_user_df)
          
          # add Surrogate Key to final_user_df
          final_user_df = final_user_df.withColumn('USER_KEY',F.lit(F.md5(F.concat(final_user_df.ID,F.lit('_'),final_user_df.START_DATE))))
          print('scd-2')
          final_user_df.show()
          final_user_df.coalesce(1).write.mode('overwrite').parquet("s3://"+silver_bucket+'/'+silver_input_folder+'/temp')
          file_list_in_silver = s3_client.list_objects_v2(Bucket=silver_bucket, Prefix=silver_input_folder, Delimiter='/')
          file_list_in_silver_temp = s3_client.list_objects_v2(Bucket=silver_bucket, Prefix=silver_input_folder+'temp/', Delimiter='/')
          for file in file_list_in_silver.get('Contents') :
            file = file.get('Key')
            if file != 'temp/' :
              s3_client.delete_object(Bucket=silver_bucket,Key=file)

          for file in file_list_in_silver_temp.get('Contents') :
            file = file.get('Key')
            s3_client.copy_object(Bucket=silver_bucket,Key=file.replace('temp/',''),
                              CopySource=silver_bucket+'/'+file)
            s3_client.delete_object(Bucket=silver_bucket,Key=file)

    # Archive File In Bronze
      file_list_in_bronze = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_input_folder+bronze_file_name+'/', Delimiter='/')
      for file in file_list_in_bronze.get('Contents') :
        file = file.get('Key')
        print(file)
        s3_client.copy_object(Bucket=bronze_bucket,Key=file.replace('input','archive'),
                              CopySource=bronze_bucket+'/'+file)
        s3_client.delete_object(Bucket=bronze_bucket,Key=file)   
      return {'StatusCode' : 200,'Message' : 'Bronze To Silver Data Load Successfully'}
    else :
      return {'StatusCode' : 400,'Message' : 'File Does Not Exists In Bronze Input Layer'}
  except Exception as e :
      for file in file_list_in_bronze.get('Contents') :
          file = file.get('Key')
          s3_client.copy_object(Bucket=bronze_bucket,Key=file.replace('input','issue'),
                                CopySource=bronze_bucket+'/'+file)
          s3_client.delete_object(Bucket=bronze_bucket,Key=file)
      return {'StatusCode' : 400,'Message' : str(e)}
  
if __name__ == "__main__" :
  try :
    json_agrs = sys.argv
    date_json = json.loads(json_agrs[-1].replace("'",'"'))
    date = date_json.get('Date')
    res = main(date)
  except :
     res = main(dt.strftime(dt.now(),'%Y-%m-%d'))
  print(res)

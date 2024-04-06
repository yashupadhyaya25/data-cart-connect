from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
import boto3
import os
import json
import sys
from datetime import datetime as dt

spark = SparkSession \
       .builder \
       .appName("product") \
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
bronze_input_folder = get_secret_value_response['product_bronze_input_folder']
bronze_archive_folder = get_secret_value_response['product_bronze_input_folder']
bronze_issue_folder = get_secret_value_response['product_bronze_input_folder']
silver_input_folder = get_secret_value_response['product_silver_folder']
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
  try :
    bronze_file_name = 'product_daily_data_'+date.replace('-','')
    file_list_in_bronze = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_input_folder+bronze_file_name+'/', Delimiter='/')
    validation_flag = file_exists_check(file_list_in_bronze)
    if validation_flag :
      try :
        silver_product_df = spark.read.option('header',True).parquet("s3://"+silver_bucket+'/'+silver_input_folder)
        silver_product_df = silver_product_df[silver_product_df['START_DATE'] != date]
      except :
        emp_RDD = spark.sparkContext.emptyRDD()
        columns = StructType([])
        silver_product_df = spark.createDataFrame(data = emp_RDD,schema=columns)
      product_df = spark.read.option('header',True).csv("s3://"+bronze_bucket+'/'+bronze_input_folder+bronze_file_name)
      if silver_product_df.count() == 0 :
        product_df = product_df.filter(col('ID') <= 15)
        product_df = product_df.withColumn(
          'PRICE',
          F.when(product_df['ID'].isin('1','2','3'),100.50)
          .otherwise(col('PRICE'))
        )
        product_df = product_df\
                    .withColumn('START_DATE',F.lit(date))\
                    .withColumn('END_DATE',F.lit('9999-12-31'))\
                    .withColumn('ACTIVE_RECORD',F.lit(1))
        product_df = product_df.withColumn('PRODUCT_KEY',F.lit(F.md5(F.concat(product_df.ID,F.lit('_'),product_df.START_DATE))))
        product_df.show()
        # Overwrite Silver Data So We Can Check SCD-2
        product_df.coalesce(1).write.mode('overwrite').parquet("s3://"+silver_bucket+'/'+silver_input_folder)
      else :
        print('Need To Implement SCD-2')
        new_product_df = product_df.join(silver_product_df,on='ID',how='left_anti')
        # new_product_df.show()
        new_product_df = new_product_df\
                    .withColumn('START_DATE',F.lit(date))\
                    .withColumn('END_DATE',F.lit('9999-12-31'))\
                    .withColumn('ACTIVE_RECORD',F.lit(1))

        product_df = product_df\
                    .withColumn('START_DATE',F.lit(date))\
                    .withColumn('END_DATE',F.lit('9999-12-31'))
        prefix = "new_"
        for column in product_df.columns:
            product_df = product_df.withColumnRenamed(column, prefix + column)

        prefix = "old_"
        for column in silver_product_df.columns:
            silver_product_df = silver_product_df.withColumnRenamed(column, prefix + column)

        updated_product_df = product_df.alias('new_data').join(silver_product_df.alias('old_data')
                            ,col('new_data.new_ID') == col('old_data.old_ID')
                            ,'inner')\
                            .where(col('new_data.new_START_DATE') <= col('old_data.old_END_DATE'))

        no_need_to_updated_product_df = product_df.alias('new_data').join(silver_product_df.alias('old_data')
                    ,col('new_data.new_ID') == col('old_data.old_ID')
                    ,'inner')\
                    .where(col('new_data.new_START_DATE') > col('old_data.old_END_DATE'))

        no_need_to_updated_product_df = no_need_to_updated_product_df.select(
            col('old_ID').alias('ID'),
            col('old_TITLE').alias('TITLE'),
            col('old_CATEGORY').alias('CATEGORY'),
            col('old_DESCRIPTION').alias('DESCRIPTION'),
            col('old_PRICE').alias('PRICE'),
            col('old_RATING_COUNT').alias('RATING_COUNT'),
            col('old_RATING_RATE').alias('RATING_RATE'),
            col('old_IMAGE_URL').alias('IMAGE_URL'),
            col('old_START_DATE').alias('START_DATE'),
            col('old_END_DATE').alias('END_DATE'),
            col('old_ACTIVE_RECORD').alias('ACTIVE_RECORD'),
        )
        # no_need_to_updated_product_df.show()
        updated_product_df_active = updated_product_df.select(
            col('new_ID').alias('ID'),
            col('new_TITLE').alias('TITLE'),
            col('new_CATEGORY').alias('CATEGORY'),
            col('new_DESCRIPTION').alias('DESCRIPTION'),
            col('new_PRICE').alias('PRICE'),
            col('new_RATING_COUNT').alias('RATING_COUNT'),
            col('new_RATING_RATE').alias('RATING_RATE'),
            col('new_IMAGE_URL').alias('IMAGE_URL'),
            col('new_START_DATE').alias('START_DATE'),
            col('new_END_DATE').alias('END_DATE'),
            F.lit(1).alias('ACTIVE_RECORD')
        )
        # updated_product_df_active.show()
        updated_product_df_inactive = updated_product_df.select(
            col('old_ID').alias('ID'),
            col('old_TITLE').alias('TITLE'),
            col('old_CATEGORY').alias('CATEGORY'),
            col('old_DESCRIPTION').alias('DESCRIPTION'),
            col('old_PRICE').alias('PRICE'),
            col('old_RATING_COUNT').alias('RATING_COUNT'),
            col('old_RATING_RATE').alias('RATING_RATE'),
            col('old_IMAGE_URL').alias('IMAGE_URL'),
            col('old_START_DATE').alias('START_DATE'),
            col('new_START_DATE').alias('END_DATE'),
            F.lit(0).alias('ACTIVE_RECORD')
        )
        # updated_product_df_inactive.show()

        final_product_df = new_product_df\
                          .union(updated_product_df_active)\
                          .union(updated_product_df_inactive)\
                          .union(no_need_to_updated_product_df)
        final_product_df = final_product_df.withColumn('PRODUCT_KEY',F.lit(F.md5(F.concat(final_product_df.ID,F.lit('_'),final_product_df.START_DATE))))
        final_product_df.coalesce(1).write.mode('overwrite').parquet("s3://"+silver_bucket+'/'+silver_input_folder+'/temp')
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

      return {'StatusCode' : 200,'Message' : 'Data Loaded Successfully From Bronze To Silver'}
    else :
      return {'StatusCode' : 400,'Message' : 'File Not Exists In Bronze Layer'}
  except Exception as e:
    for file in file_list_in_bronze.get('Contents') :
          file = file.get('Key')
          s3_client.copy_object(Bucket=bronze_bucket,Key=file.replace('input','issue'),
                                CopySource=bronze_bucket+'/'+file)
          s3_client.delete_object(Bucket=bronze_bucket,Key=file)
    return {'StatusCode' : 400,'Message' : str(e)}

if __name__ == '__main__' :
    json_agrs = sys.argv
    args_json = json.loads(json_agrs[-1].replace("'",'"'))
    try :
      date = args_json.get('Date')
      res = main(date)
    except :
      res = main(dt.strftime(dt.now(),'%Y-%m-%d'))
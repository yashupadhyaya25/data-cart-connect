import requests as rq
from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd

date = dt.strftime(dt.now(),'%Y-%m-%d')


def get_product():
        api_url = 'https://fakestoreapi.com/products'
        response = rq.get(api_url)
        if response.status_code != 200 :
            return {'statusCode': str(response.status_code),'Message' : 'API encountered an error'}
        json_data = response.json()
        product_df = pd.json_normalize(json_data)
        product_df.to_csv('s3_data/product/product_daily_data_'+date.replace('-','')+'.csv',index=False)

if __name__ == '__main__' :
      get_product()
import boto3
import requests
import pandas as pd

def lambda_handler(event, context):
    print(event)

    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url = object_get_context["inputS3Url"]

    # Get object from S3
    response = requests.get(s3_url)
    original_object = response.content.decode('utf-8')

    # Transform object
    columns = original_object.split('\r\n')[0].replace('"','').replace('\ufeff','').split(',')
    user_data = []
    for data in original_object.split('\r\n')[1:-1] :
        user_data.append(data.replace('"','').split(','))
    user_df = pd.DataFrame(user_data,columns=columns)
    user_df.drop(columns=['password'],inplace=True)

    # Write object back to S3 Object Lambda
    s3 = boto3.client('s3')
    s3.write_get_object_response(
        Body=bytes(user_df.to_csv(index=False),encoding = 'utf-8'),
        RequestRoute=request_route,
        RequestToken=request_token)

    return {'status_code': 200}
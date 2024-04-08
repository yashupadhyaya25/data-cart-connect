from aws_cdk import (
    Stack,
    aws_secretsmanager as secret_manager
)
from constructs import Construct
import json

class AwsSecretManagerStack(Stack) :
    def __init__(self, scope: Construct, id: str,**kwargs) -> None:
        super().__init__(scope, id,**kwargs)

        data_cart_connect = secret_manager.Secret(
            self,
            id = 'SM_DataCartConnect',
            description= 'All S3 path and other config are store here',
            secret_name= 'SM_DataCartConnect',
        ) 
        data_cart_connect.secret_value_from_json(json_field=
                                    """{
                                    'product_raw_input_folder': 'raw/product/input/'
                                    "product_raw_issue_folder": "raw/product/issue/",
                                    "product_raw_archive_folder": "raw/product/archive/",
                                    "product_bronze_input_folder": "bronze/product/input/",
                                    "product_bronze_issue_folder": "bronze/product/issue/",
                                    "product_bronze_archive_folder": "bronze/product/archive/",
                                    "product_silver_folder": "silver/product/",
                                    "cart_raw_input_folder": "raw/cart/input/",
                                    "cart_raw_issue_folder": "raw/cart/issue/",
                                    "cart_raw_archive_folder": "raw/cart/archive/",
                                    "cart_bronze_input_folder": "bronze/cart/input/",
                                    "cart_bronze_issue_folder": "bronze/cart/issue/",
                                    "cart_bronze_archive_folder": "bronze/cart/archive/",
                                    "cart_silver_folder": "silver/cart/",
                                    "user_raw_input_folder": "raw/user/input/",
                                    "user_raw_issue_folder": "raw/user/issue/",
                                    "user_raw_archive_folder": "raw/user/archive/",
                                    "user_bronze_input_folder": "bronze/user/input/",
                                    "user_bronze_issue_folder": "bronze/user/issue/",
                                    "user_bronze_archive_folder": "bronze/user/archive/",
                                    "user_silver_folder": "silver/user/",
                                    "raw_bucket" : "datacartconnectraw",
                                    "bronze_bucket" : "datacartconnectbronze",
                                    "silver_bucket" : "datacartconnectsilver"
                                }"""
                                                 )
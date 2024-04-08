from aws_cdk import (
    Stack,
    aws_secretsmanager as secret_manager,
    SecretValue
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
            secret_object_value={
                                    'product_raw_input_folder': SecretValue.unsafe_plain_text('raw/product/input/'),
                                    'product_raw_issue_folder': SecretValue.unsafe_plain_text('raw/product/issue/'),
                                    'product_raw_archive_folder': SecretValue.unsafe_plain_text('raw/product/archive/'),
                                    'product_bronze_input_folder': SecretValue.unsafe_plain_text('bronze/product/input/'),
                                    'product_bronze_issue_folder': SecretValue.unsafe_plain_text('bronze/product/issue/'),
                                    'product_bronze_archive_folder': SecretValue.unsafe_plain_text('bronze/product/archive/'),
                                    'product_silver_folder': SecretValue.unsafe_plain_text('silver/product/'),
                                    'cart_raw_input_folder': SecretValue.unsafe_plain_text('raw/cart/input/'),
                                    'cart_raw_issue_folder': SecretValue.unsafe_plain_text('raw/cart/issue/'),
                                    'cart_raw_archive_folder': SecretValue.unsafe_plain_text('raw/cart/archive/'),
                                    'cart_bronze_input_folder': SecretValue.unsafe_plain_text('bronze/cart/input/'),
                                    'cart_bronze_issue_folder': SecretValue.unsafe_plain_text('bronze/cart/issue/'),
                                    'cart_bronze_archive_folder': SecretValue.unsafe_plain_text('bronze/cart/archive/'),
                                    'cart_silver_folder': SecretValue.unsafe_plain_text('silver/cart/'),
                                    'user_raw_input_folder': SecretValue.unsafe_plain_text('raw/user/input/'),
                                    'user_raw_issue_folder': SecretValue.unsafe_plain_text('raw/user/issue/'),
                                    'user_raw_archive_folder': SecretValue.unsafe_plain_text('raw/user/archive/'),
                                    'user_bronze_input_folder': SecretValue.unsafe_plain_text('bronze/user/input/'),
                                    'user_bronze_issue_folder': SecretValue.unsafe_plain_text('bronze/user/issue/'),
                                    'user_bronze_archive_folder': SecretValue.unsafe_plain_text('bronze/user/archive/'),
                                    'user_silver_folder': SecretValue.unsafe_plain_text('silver/user/'),
                                    'raw_bucket' : SecretValue.unsafe_plain_text('datacartconnectraw'),
                                    'bronze_bucket' : SecretValue.unsafe_plain_text('datacartconnectbronze'),
                                    'silver_bucket' : SecretValue.unsafe_plain_text('datacartconnectsilver')
                                }
        ) 
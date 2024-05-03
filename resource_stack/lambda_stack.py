from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam
)
from constructs import Construct
from config.config import _config

config_obj = _config('prod')
config_values = config_obj.get_config()
raw_bucket = config_values.get('raw_bucket')
user_folder_path = config_values.get('lambda_user_raw_input_path')
cart_folder_path = config_values.get('lambda_cart_raw_input_path')
product_folder_path = config_values.get('lambda_product_raw_input_path')

class AwsLambdaStack(Stack) :
    def __init__(self, scope: Construct, id: str,**kwargs) -> None:
        super().__init__(scope, id,**kwargs)

        lambda_role = iam.Role.from_role_arn(self,'Lambda',role_arn='arn:aws:iam::975050311718:role/Lambda')
        request_layer = _lambda.LayerVersion(
            self,
            id = 'request_layer',
            description= 'request module for calling api',
            layer_version_name= 'request',
            code=_lambda.Code.from_asset('./lambda_layers/request/'),
            compatible_architectures= [_lambda.Architecture.ARM_64,_lambda.Architecture.X86_64],
            compatible_runtimes= [_lambda.Runtime.PYTHON_3_12]
        )

        user_lambda = _lambda.Function(
            self,
            id = 'user',
            code = _lambda.Code.from_asset('./lambda_scripts/user'),
            handler= 'get_user.lambda_handler',
            runtime= _lambda.Runtime.PYTHON_3_12,
            role= lambda_role,
            function_name= 'get_user',
            layers=[request_layer],
            environment= {'bucket' : raw_bucket,'folder' : user_folder_path}
        )

        cart_lambda = _lambda.Function(
            self,
            id = 'cart',
            code = _lambda.Code.from_asset('./lambda_scripts/cart'),
            handler= 'get_cart.lambda_handler',
            runtime= _lambda.Runtime.PYTHON_3_12,
            role= lambda_role,
            function_name= 'get_cart',
            layers=[request_layer],
            environment= {'bucket' : raw_bucket,'folder' : cart_folder_path}
        )
        
        product_lambda = _lambda.Function(
            self,
            id = 'product',
            code = _lambda.Code.from_asset('./lambda_scripts/product'),
            handler= 'get_product.lambda_handler',
            runtime= _lambda.Runtime.PYTHON_3_12,
            role= lambda_role,
            function_name= 'get_product',
            layers=[request_layer],
            environment= {'bucket' : raw_bucket,'folder' : product_folder_path}
        )

        check_object_exists_in_s3 = _lambda.Function(
            self,
            id = 'check_object_exists_in_s3',
            code = _lambda.Code.from_asset('./lambda_scripts/check_object_exists_in_s3'),
            handler= 'object_exists_in_s3.lambda_handler',
            runtime= _lambda.Runtime.PYTHON_3_12,
            role= lambda_role,
            function_name= 'check_object_exists_in_s3'
        )
from aws_cdk import (
    Stack,
    aws_s3 as s3
)
from constructs import Construct

class AwsS3BucketStack(Stack) :
    def __init__(self, scope: Construct, id: str,**kwargs) -> None:
        super().__init__(scope, id,**kwargs)

        raw_bucket = s3.Bucket(
            self,
            id = 'datacartconnectraw',
            bucket_name= 'datacartconnectraw'
        )

        bronze_bucket = s3.Bucket(
            self,
            id = 'datacartconnectbronze',
            bucket_name= 'datacartconnectbronze'
        )

        silver_bucket = s3.Bucket(
            self,
            id = 'datacartconnectsilver',
            bucket_name= 'datacartconnectsilver'
        )
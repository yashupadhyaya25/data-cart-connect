from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment
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

        emr_script_bucket = s3.Bucket(
            self,
            id = 'datacartconnectemrscript',
            bucket_name= 'datacartconnectemrscript'
        )

        ## Add the script to datacartconnectemrscript
        s3_deployment.BucketDeployment(
            self,
            id = 'emrscript',
            sources=[s3_deployment.Source.asset('./emr_script')],
            destination_bucket= emr_script_bucket
        )
        
from aws_cdk import (
    Stack,
    Stage,
    pipelines,
    aws_codepipeline as codepipeline
)
from constructs import Construct
from resource_stack.lambda_stack import AwsLambdaStack
from resource_stack.s3_stack import AwsS3BucketStack
from resource_stack.secret_manager_stack import AwsSecretManagerStack

class LambdaDeployStage(Stage) :
    def __init__(self,scope : Construct, construct_id: str) :
        super().__init__(scope,construct_id)
        AwsLambdaStack(self,
                      'LambdaStack',
                    #   env=env,
                      stack_name = 'lambda-stack-deploy'
                      )

class S3DeployStage(Stage) :
    def __init__(self,scope : Construct, construct_id: str) :
        super().__init__(scope,construct_id)
        AwsS3BucketStack(self,
                      'AwsS3BucketStack',
                    #   env=env,
                      stack_name = 'S3-stack-deploy'
                      )

class SecretManagerDeployStage(Stage) :
    def __init__(self,scope : Construct, construct_id: str) :
        super().__init__(scope,construct_id)
        AwsSecretManagerStack(self,
                      'AwsSecretManagerStack',
                    #   env=env,
                      stack_name = 'secret-manager-stack-deploy'
                      )

class AwsCodePipeline(Stack) :
    def __init__(self,scope : Construct, construct_id: str) :
        super().__init__(scope,construct_id)
    
        git_input = pipelines.CodePipelineSource.connection(
            repo_string="yashupadhyaya25/DataCartConnect",
            branch="main",
            connection_arn="arn:aws:codestar-connections:eu-north-1:975050311718:connection/5b72fd28-0064-462e-bbda-0aba0d4b4be1"
        )

        code_pipeline = codepipeline.Pipeline(
            self, "DataCartConnect",
            pipeline_name="DataCartConnect-Pipeline",
            cross_account_keys=False
        )

        synth_step = pipelines.ShellStep(
            "Synth",
            install_commands=[
                'pip install -r requirements.txt'
            ],
            commands=[
                'npx cdk synth'
            ],
            input=git_input
        )

        pipeline = pipelines.CodePipeline(
            self, 'CodePipeline',
            self_mutation=True,
            code_pipeline=code_pipeline,
            synth=synth_step
        )

        deployment_wave = pipeline.add_wave("LambdaDeploymentWave")

        deployment_wave.add_stage(LambdaDeployStage(
            self,'LambdaDeployStage'
            # env=(Environment(account='975050311718', region='eu-north-1'))
        ))

        s3_deployment_wave = pipeline.add_wave("S3DeploymentWave")

        s3_deployment_wave.add_stage(S3DeployStage(
            self,'S3DeployStage'
            # env=(Environment(account='975050311718', region='eu-north-1'))
        ))

        # secret_manager_deployment_wave = pipeline.add_wave("SecretManagerDeploymentWave")

        # secret_manager_deployment_wave.add_stage(SecretManagerDeployStage(
        #     self,'SecretManagerDeployStage'
        #     # env=(Environment(account='975050311718', region='eu-north-1'))
        # ))
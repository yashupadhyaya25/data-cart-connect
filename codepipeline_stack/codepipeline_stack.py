from aws_cdk import (
    Stack,
    Stage,
    Environment,
    pipelines,
    aws_codepipeline as codepipeline
)
from constructs import Construct
from resource_stack.lambda_stack import AwsLambdaStack

class LambdaDeployStage(Stage) :
    def __init__(self,scope : Construct, construct_id: str) :
        super().__init__(scope,construct_id)
        AwsLambdaStack(self,
                      'LambdaStack',
                    #   env=env,
                      stack_name = 'lambda-stack-deploy'
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
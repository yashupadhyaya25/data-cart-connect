#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_cart_connect.data_cart_connect_stack import DataCartConnectStack
from codepipeline_stack.codepipeline_stack import AwsCodePipeline

app = cdk.App()
AwsCodePipeline(app, "AwsCodePipeline")

app.synth()

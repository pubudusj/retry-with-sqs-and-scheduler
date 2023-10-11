#!/usr/bin/env python3
import os

import aws_cdk as cdk

from sqs_controlled_retry.sqs_controlled_retry_stack import SqsControlledRetryStack


app = cdk.App()
SqsControlledRetryStack(app, "SqsControlledRetryStack")

app.synth()

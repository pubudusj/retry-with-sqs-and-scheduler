import aws_cdk as core
import aws_cdk.assertions as assertions

from sqs_controlled_retry.sqs_controlled_retry_stack import SqsControlledRetryStack

# example tests. To run these tests, uncomment this file along with the example
# resource in sqs_controlled_retry/sqs_controlled_retry_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = SqsControlledRetryStack(app, "sqs-controlled-retry")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })

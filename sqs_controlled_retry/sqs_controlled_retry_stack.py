from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_iam as iam,
)
from constructs import Construct


class SqsControlledRetryStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Intermediate Dead Letter Queue
        intermediate_dlq = sqs.Queue(self, "IntermediateDLQ")

        # Intermediate Dead Letter Queue
        source_queue = sqs.Queue(
            self,
            "SourceQueue",
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=1, queue=intermediate_dlq
            ),
            visibility_timeout=Duration.seconds(15),
        )

        # Final Dead Letter Queue
        final_dlq = sqs.Queue(self, "FinalDLQ")

        # Message Processing Lambda function
        message_processing_lambda = lambda_.Function(
            scope=self,
            id="MessageProcessingLambdaFunction",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="index.event_handler",
            code=lambda_.Code.from_asset(
                "src/message_processing_lambda",
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_10.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            timeout=Duration.seconds(10),
            retry_attempts=0,
            memory_size=128,
            environment={
                "FINAL_DLQ_URL": final_dlq.queue_url,
            },
        )
        # Add sqs source queue as event source
        message_processing_lambda.add_event_source(
            event_sources.SqsEventSource(source_queue, batch_size=1)
        )
        # Add send message permission for final DLQ
        final_dlq.grant_send_messages(message_processing_lambda)

        # Scheduler permissions
        scheduler_role = iam.Role(
            self,
            "scheduler-role",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        scheduler_events_policy = iam.PolicyStatement(
            actions=["sqs:SendMessage"],
            resources=[source_queue.queue_arn],
            effect=iam.Effect.ALLOW,
        )
        scheduler_role.add_to_policy(scheduler_events_policy)

        # Failed Message Processing Lambda function
        failed_message_processing_lambda = lambda_.Function(
            scope=self,
            id="FailedMessageProcessingLambdaFunction",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="index.event_handler",
            code=lambda_.Code.from_asset("src/failed_message_processing_lambda"),
            timeout=Duration.seconds(10),
            memory_size=128,
            environment={
                "TARGET_QUEUE_ARN": source_queue.queue_arn,
                "FINAL_DLQ_URL": final_dlq.queue_url,
                "SCHEDULER_ROLE_ARN": scheduler_role.role_arn,
            },
            retry_attempts=0,
        )

        # Add intermediate dlq as event source
        failed_message_processing_lambda.add_event_source(
            event_sources.SqsEventSource(intermediate_dlq, batch_size=1)
        )

        # Add send message permission for final DLQ
        final_dlq.grant_send_messages(failed_message_processing_lambda)

        failed_message_processing_lambda.role.attach_inline_policy(
            iam.Policy(
                self,
                "create-schedule-policy",
                statements=[
                    iam.PolicyStatement(
                        actions=["scheduler:CreateSchedule"],
                        resources=["*"],
                        effect=iam.Effect.ALLOW,
                    )
                ],
            )
        )
        scheduler_role.grant_pass_role(failed_message_processing_lambda)

        # Output
        CfnOutput(self, "SourceSQSQueue", value=source_queue.queue_arn)
        CfnOutput(self, "FinalDLQueue", value=final_dlq.queue_arn)
        CfnOutput(self, "IntermediateDLQueue", value=intermediate_dlq.queue_arn)
        CfnOutput(
            self,
            "MessageProcessingLambdaFunctionArn",
            value=message_processing_lambda.function_arn,
        )
        CfnOutput(
            self,
            "FailedMessageProcessingLambdaFunctionArn",
            value=failed_message_processing_lambda.function_arn,
        )

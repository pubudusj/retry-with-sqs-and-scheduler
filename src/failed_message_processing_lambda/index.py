import json
import os
import boto3
from datetime import datetime, timedelta
import uuid

sqs = boto3.client("sqs")
scheduler = boto3.client("scheduler")

MAX_RETRY_ATTEMPTS = 5
MAX_MESSAGE_AGE_IN_SEC = 3600
FINAL_DLQ_URL = os.environ["FINAL_DLQ_URL"]
TARGET_QUEUE_ARN = os.environ["TARGET_QUEUE_ARN"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]


class MaxRetryAttemptsExceededException(Exception):
    """Max retry attempts exceeded exception"""


class MessageAgeExceededException(Exception):
    """Message age exceeded exception"""


def _increment_retry_attempt(event):
    if "retry_attempt" in event["metadata"]:
        event["metadata"]["retry_attempt"] += 1
    else:
        event["metadata"]["retry_attempt"] = 1

    return event


def _check_if_max_retry_attempts_exceed(message):
    if message["metadata"]["retry_attempt"] > MAX_RETRY_ATTEMPTS:
        raise MaxRetryAttemptsExceededException(
            f"Max retry attempts {MAX_RETRY_ATTEMPTS} exceeded"
        )


def _send_to_final_dlq(original_message, error_type: str, exception: Exception):
    sqs.send_message(
        QueueUrl=FINAL_DLQ_URL,
        MessageBody=json.dumps(original_message),
        MessageAttributes={
            "ErrorType": {
                "DataType": "String",
                "StringValue": error_type,
            },
            "ErrorDetails": {
                "DataType": "String",
                "StringValue": str(exception),
            },
        },
    )
    print("Message was sent to final DLQ")


def _calculate_next_retry_time(message):
    retry_attempt = message["metadata"]["retry_attempt"]
    new_datetime = datetime.now() + timedelta(seconds=(60 * retry_attempt))
    message["metadata"]["next_retry_time"] = new_datetime.strftime(
        "%Y-%m-%dT%H:%M:00.%fZ"
    )

    return new_datetime, message


def _create_schedule(message, next_retry_time):
    scheduler.create_schedule(
        Name=str(uuid.uuid4()),
        ScheduleExpression=f"at({next_retry_time.strftime('%Y-%m-%dT%H:%M')})",
        FlexibleTimeWindow={"Mode": "OFF"},
        ActionAfterCompletion="DELETE",
        State="ENABLED",
        Target={
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Arn": TARGET_QUEUE_ARN,
            "Input": json.dumps(message),
        },
        Description=f"Schedule for message retry: {message['metadata']['message_id']}",
    )


def event_handler(event, context):
    event = event["Records"][0]
    message = json.loads(event["body"])

    # increment or add retry attempt
    _increment_retry_attempt(message)

    try:
        # Check no of retries exceed
        _check_if_max_retry_attempts_exceed(message)
    except MaxRetryAttemptsExceededException as e:
        print("Max retry attempts exceeded")
        _send_to_final_dlq(message, "RETRY_ATTEMPT_EXCEEDED", e)
        return

    # Calculate next retry time
    next_retry_time, message = _calculate_next_retry_time(message)

    # Create schedule with next retry time
    _create_schedule(message, next_retry_time)

    print(
        {
            "message_id": message["metadata"]["message_id"],
            "retry_attempt": message["metadata"]["retry_attempt"],
            "next_retry_time": message["metadata"]["next_retry_time"],
        }
    )

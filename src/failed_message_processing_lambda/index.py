import json
import os
import boto3
from datetime import datetime, timedelta
import uuid

sqs = boto3.client("sqs")
scheduler = boto3.client("scheduler")

MAX_RETRY_COUNT = 5
MAX_MESSAGE_AGE_IN_SEC = 3600
FINAL_DLQ_URL = os.environ["FINAL_DLQ_URL"]
TARGET_QUEUE_ARN = os.environ["TARGET_QUEUE_ARN"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]


class RetryCountExceededException(Exception):
    """Retry count exceeded exception"""


class MessageAgeExceededException(Exception):
    """Message age exceeded exception"""


def _increment_retry_count(event):
    if "retry_count" in event["metadata"]:
        event["metadata"]["retry_count"] += 1
    else:
        event["metadata"]["retry_count"] = 1

    return event


def _check_if_max_retry_attempts_exceed(message):
    if message["metadata"]["retry_count"] > MAX_RETRY_COUNT:
        raise RetryCountExceededException(f"Max retry count {MAX_RETRY_COUNT} exceeded")


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
    retry_count = message["metadata"]["retry_count"]
    new_datetime = datetime.now() + timedelta(seconds=(60 * retry_count))
    message["metadata"]["next_retry_time"] = new_datetime.strftime(
        "%Y-%m-%dT%H:%M:00.%fZ"
    )

    return new_datetime, message


def _create_schedule(message, next_retry_time):
    scheduler.create_schedule(
        Name=str(uuid.uuid4()),
        ScheduleExpression=f"at({next_retry_time.strftime('%Y-%m-%dT%H:%M')})",
        FlexibleTimeWindow={"Mode": "OFF"},
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

    # increment or add retry count
    _increment_retry_count(message)

    try:
        # Check no of retries exceed
        _check_if_max_retry_attempts_exceed(message)
    except RetryCountExceededException as e:
        print("Max retry count exceeded")
        _send_to_final_dlq(message, "RETRY_COUNT_EXCEEDED", e)
        return

    # Calculate next retry time
    next_retry_time, message = _calculate_next_retry_time(message)

    # Create schedule with next retry time
    _create_schedule(message, next_retry_time)

    print(
        {
            "message_id": message["metadata"]["message_id"],
            "retry_count": message["metadata"]["retry_count"],
            "next_retry_time": message["metadata"]["next_retry_time"],
        }
    )

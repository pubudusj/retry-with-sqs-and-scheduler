import jsonschema
import json
import boto3
import os
from datetime import datetime
import uuid


sqs = boto3.client("sqs")
FINAL_DLQ_URL = os.environ["FINAL_DLQ_URL"]


# validate event with jsonschema
def _validate_payload(body):
    message_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {
            "metadata": {
                "type": "object",
                "properties": {
                    "message_id": {
                        "type": "string",
                        "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
                    },
                },
                "required": ["message_id"],
            },
            "data": {"type": "object"},
        },
        "required": ["metadata", "data"],
    }

    jsonschema.validate(instance=body, schema=message_schema)


def send_to_final_dlq(original_message, error_type: str, exception: Exception):
    sqs.send_message(
        QueueUrl=FINAL_DLQ_URL,
        MessageBody=original_message,
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


def event_handler(event, context):
    print("Message received")
    # Batch size is 1
    event = event["Records"][0]
    exception = False
    try:
        body = json.loads(event["body"])
        print(body)
        _validate_payload(body)
    except json.JSONDecodeError as e:
        send_to_final_dlq(event["body"], "INVALID_MESSAGE_FORMAT", e)
        exception = True
    except jsonschema.ValidationError as e:
        send_to_final_dlq(event["body"], "INVALID_MESSAGE_SCHEMA", e)
        exception = True

    # If message is valid, raise the intentional exception to simulate retry
    if not exception:
        raise Exception("This exception is intentionally thrown")

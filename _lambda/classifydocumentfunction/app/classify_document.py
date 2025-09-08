"""
Classify documents based on their content or calling an custom classifier endpoint
"""
import json
import logging
import os
import boto3

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    # TODO: Implement document classification logic here
    # from event read S3Path, mime_type
    s3_path = event.get("manifest", {}).get("s3Path", "")
    mime_type = event.get("mime", "")
    logger.info(f"S3Path: {s3_path}, MimeType: {mime_type}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_path": s3_path,
            "mime_type": mime_type,
            "classification": "accounts-payable"
        })
    }


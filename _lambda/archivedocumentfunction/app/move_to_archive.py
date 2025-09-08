"""
Moving files to the landing zone with proper prefixes
"""

import json
import logging
import os
import boto3
from datetime import datetime
import re

logger = logging.getLogger(__name__)

archive_prefix = "archive"

def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    
    body = event.get("Payload", {}).get("body", "")
    body_dict = json.loads(body)
    s3_path = body_dict.get("s3_path", "")
    mime_type = body_dict.get("mime_type", "")
    s3_bucket = s3_path.split('/')[2]
    source_key = body_dict.get("source_key", "")

    current_time = datetime.utcnow()
    timestamp = current_time.isoformat()
    year = current_time.strftime("%Y")
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")

    
    destination_key = f"{archive_prefix}/{year}/{month}/{day}/{source_key.split('/')[-1]}"

    logger.info(f"S3Path: {s3_path}, MimeType: {mime_type}, "
                f"S3Bucket: {s3_bucket}, "
                f"SourceKey: {source_key}, "
                f"DestinationKey: {destination_key}")

    try:
        s3_client = boto3.client("s3")
        
        # Copy the object to the archive folder
        new_s3_key = destination_key
        copy_source = {'Bucket': s3_bucket, 'Key': source_key}
        s3_client.copy(copy_source, s3_bucket, new_s3_key)
        logger.info(f"File copied to {s3_bucket}/{new_s3_key}")

        # Delete the original object
        s3_client.delete_object(Bucket=s3_bucket, Key=source_key)
        logger.info(f"Original file {s3_bucket}/{source_key} deleted")


        return {
            'statusCode': 200,
            's3_path': f"s3://{s3_bucket}/{new_s3_key}",
            'source_key': source_key,
            's3_bucket': s3_bucket,
            'new_s3_key': new_s3_key,
            'mime_type': mime_type,
            'destination_key': destination_key
        }

    except Exception as e:
        logger.error(e)
        raise e
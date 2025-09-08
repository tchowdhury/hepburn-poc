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

landing_prefix = "landing"

def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    
    body = event.get("Payload", {}).get("body", "")
    body_dict = json.loads(body)
    s3_path = body_dict.get("s3_path", "")
    mime_type = body_dict.get("mime_type", "")
    classification = body_dict.get("classification", "")
    s3_bucket = s3_path.split('/')[2]
    original_filename = s3_path.split('/')[-1]
    new_file_name = original_filename.split('.')[0] + '_' + datetime.utcnow().replace(microsecond=0).isoformat() + '.' + original_filename.split('.')[-1]
    logger.info(f"S3Path: {s3_path}, MimeType: {mime_type}, "
                f"Classification: {classification}, S3Bucket: {s3_bucket}, "
                f"OriginalFilename: {original_filename}, NewFileName: {new_file_name}")

    try:
        s3_client = boto3.client("s3")
        current_time = datetime.utcnow()
        timestamp = current_time.isoformat()
        year = current_time.strftime("%Y")
        month = current_time.strftime("%m")
        day = current_time.strftime("%d")

        new_s3_key = f"{landing_prefix}/{classification}/{year}/{month}/{day}/{new_file_name}"
        logger.info(f"New S3 Key: {new_s3_key}")

        # Parse bucket and key from s3_path
        
        match = re.match(r"s3://([^/]+)/(.+)", s3_path)
        if match:
            source_bucket = match.group(1)
            source_key = match.group(2)
        else:
            raise ValueError("Invalid S3 path format")

        s3_client.copy_object(
            Bucket=s3_bucket,
            Key=new_s3_key,
            CopySource=f"{source_bucket}/{source_key}"
        )
        logger.info(f"File moved to landing zone: s3://{s3_bucket}/{new_s3_key}")

        return {
            'statusCode': 200,
            's3_path': f"s3://{s3_bucket}/{new_s3_key}",
            'source_bucket': source_bucket,
            'source_key': source_key,
            's3_bucket': s3_bucket,
            'new_s3_key': new_s3_key,
            'mime_type': mime_type,
            'classification': classification,
            'newFileName': new_file_name
        }

    except Exception as e:
        logger.error(e)
        raise e
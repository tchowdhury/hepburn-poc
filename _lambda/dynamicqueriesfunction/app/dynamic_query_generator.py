"""
    Performs a smart match of a filename against keys in an S3 mapping file using enhanced regex patterns.
"""

import json
import logging
import os
from urllib import response
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

    
    body_dict = event.get("Payload", {})
    s3_path = body_dict.get("s3_path", "")
    source_bucket = body_dict.get("source_bucket", "")
    source_key = body_dict.get("source_key", "")
    s3_bucket = body_dict.get("s3_bucket", "")
    new_s3_key = body_dict.get("new_s3_key", "")
    mime_type = body_dict.get("mime_type", "")
    classification = body_dict.get("classification", "")
    new_file_name = body_dict.get("newFileName", "")
    
    
    logger.info(f"S3Path: {s3_path}, MimeType: {mime_type}, "
                f"Classification: {classification}, S3Bucket: {s3_bucket}, "
                f"OriginalFilename: {source_key}, NewFileName: {new_file_name}")

    try:
        s3_client = boto3.client("s3")
        
        fl_name = os.path.basename(source_key).lower()
        vendor_mapping_key = "query_config/vendor_mapping.json"
        queries_config_key = "query_config/query.json"
        logger.info(f"Filename: {fl_name}")

        vendor_mapping_response = s3_client.get_object(Bucket=s3_bucket, Key=vendor_mapping_key)
        mapping_data = json.loads(vendor_mapping_response['Body'].read().decode('utf-8'))
        print(f"Vendor mapping data: {mapping_data}")

        queries_config_response = s3_client.get_object(Bucket=s3_bucket, Key=queries_config_key)
        queries_data = json.loads(queries_config_response['Body'].read().decode('utf-8'))
        print(f"Queries data: {queries_data}")

        vendor = "default"
        for key, value in mapping_data.items():
            # Choose the appropriate pattern based on the key's structure
            if "^" in key and "$" in key:  # Exact match
                pattern = re.compile(key, re.IGNORECASE)
            elif " " in key:  # Partial match with word boundaries
                pattern = re.compile(r"\b" + key.replace(" ", r"\b\s*\b") + r"\b", re.IGNORECASE)
            elif "INVOICE" in key and re.search(r"\d", key): # Partial match with invoice numbers
                pattern = re.compile(key, re.IGNORECASE)
            else: # Default to simple matching
                pattern = re.compile(key, re.IGNORECASE)

            if pattern.search(fl_name):
                vendor = value
                break

        print(f"Vendor matched: {vendor}")        
        query = queries_data.get(vendor, "default")

        
        return {
            'statusCode': 200,
            's3_path': s3_path,
            'source_bucket': source_bucket,
            'source_key': source_key,
            's3_bucket': s3_bucket,
            'new_s3_key': new_s3_key,
            'newFileName': new_file_name,
            'classification': classification,
            'query': query,
            'mime_type': mime_type,
        }

    except Exception as e:
        logger.error(e)
        raise e
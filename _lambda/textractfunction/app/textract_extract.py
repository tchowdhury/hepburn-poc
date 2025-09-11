"""
    StartDocumentAnalysis + poll GetDocumentAnalysis pages until SUCCEEDED or timeout.
    Returns the fully merged response dict (with combined Blocks list).
 """

import json
import logging
import os
import boto3
from datetime import datetime
import time


logger = logging.getLogger(__name__)

raw_prefix = "raw"
adapter_id = os.environ.get('ADAPTER_ID', 'default-adapter-id')
version = os.environ.get('VERSION', '1.0')

AWS_REGION = os.environ.get('AWS_REGION')

textract = boto3.client("textract", region_name=AWS_REGION)

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
    new_file_name = body_dict.get("newFileName", "")
    classification = body_dict.get("classification", "")
    query = body_dict.get("query", []) 
    tmp_extract_prefix = f"{raw_prefix}/textract-output"
    max_wait_secs = 900
    poll_interval_secs = 5

    logger.info(f"S3Path: {s3_path}, SourceBucket: {source_bucket}, region: {AWS_REGION}, "
                f"Classification: {classification}, S3Bucket: {s3_bucket}, "
                f"OriginalFilename: {source_key}, NewFileName: {new_file_name}")

    try:
        
        start_kwargs = {
        "DocumentLocation": {"S3Object": {"Bucket": s3_bucket, "Name": new_s3_key}},
        "FeatureTypes": ["QUERIES", "TABLES"],
        "QueriesConfig": {"Queries": query},
        "AdaptersConfig": {"Adapters": [{"AdapterId": adapter_id}]},
        "OutputConfig": {
            "S3Bucket": s3_bucket,
            "S3Prefix": tmp_extract_prefix
        }
        }

        if version:
            start_kwargs["AdaptersConfig"]["Adapters"][0]["Version"] = version

        job_id = textract.start_document_analysis(**start_kwargs)["JobId"] 

        print("Started job with id:", job_id)

        waited = 0
        status = "IN_PROGRESS"
        poll_secs = poll_interval_secs

        while True:
            job_status = textract.get_document_analysis(JobId=job_id)
            status = job_status.get("JobStatus")
            if status in ["SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"]:
                break
            logging.info(f"Current job status: {status}. Waiting 5 seconds...")
            time.sleep(5)

        if status != "SUCCEEDED":
            raise Exception(f"Textract job did not succeed. Status: {status}")

        return {
            'statusCode': 200,
            's3_path': s3_path,
            'source_bucket': source_bucket,
            'source_key': source_key,
            's3_bucket': s3_bucket,
            'new_s3_key': new_s3_key,
            'newFileName': new_file_name,
            'classification': classification,
            'job_id': job_id,
            'status': status,
            'textract_output_s3_prefix': tmp_extract_prefix
        }

    except Exception as e:
        logger.error(e)
        raise e
"""
Copy Textract output to raw zone
"""
import json
import logging
import os
import boto3


logger = logging.getLogger(__name__)
s3_client = boto3.client("s3")
paginator = s3_client.get_paginator("list_objects_v2")
file_keys = []

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
    new_file_name = body_dict.get("newFileName", "")
    classification = body_dict.get("classification", "")
    job_id = body_dict.get("job_id", "")
    textract_output_s3_prefix = body_dict.get("textract_output_s3_prefix", "")

    try:
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=f"{textract_output_s3_prefix}/{job_id}/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Skip "folder" placeholders and .s3_access_check
                if key.endswith("/") or key.endswith(".s3_access_check") or key.split("/")[-1] == ".s3_access_check":
                    continue
                file_keys.append(key)
        
        # Loop through each part file (page) and copy to the new location
        for key in file_keys:
            new_key = key.replace(f"{textract_output_s3_prefix}/{job_id}/", f"raw/{classification}/{new_file_name}/")
            logger.info(f"Copying {key} to {new_key}")
            s3_client.copy_object(
                Bucket=s3_bucket,
                CopySource={"Bucket": s3_bucket, "Key": key},
                Key=new_key
            )
        

        return {
            "statusCode": 200,
            "s3_path": s3_path,
            "source_bucket": source_bucket,
            "source_key": source_key,
            "s3_bucket": s3_bucket,
            "newFileName": new_file_name,
            "classification": classification,
                
        }
    
    except Exception as e:
        logger.error(f"Error processing file {source_key} from bucket {source_bucket}. Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }   

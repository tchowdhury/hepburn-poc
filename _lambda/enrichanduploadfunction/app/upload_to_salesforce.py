"""
Upload to Salesforce Lambda Function
"""
import json
import logging
import os
import boto3
import requests

logger = logging.getLogger(__name__)

s3 = boto3.client('s3')

api_url = os.environ.get('API_ENDPOINT', '')

def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    body_dict = event.get("Payload", {})
    s3_path = body_dict.get("s3_path", "")
    source_bucket = body_dict.get("source_bucket", "")
    s3_bucket = body_dict.get("s3_bucket", "")
    source_key = body_dict.get("source_key", "")
    new_file_name = body_dict.get("newFileName", "")
    classification = body_dict.get("classification", "")
    prefix = f"processed/{classification}/{new_file_name}/"

    try:

        # List objects in the S3 folder
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

        # Check if any objects were found
        if 'Contents' not in response:
            logger.info(f"No objects found in bucket '{s3_bucket}' with prefix '{prefix}'")
        else:
            logger.info(f"Found {len(response['Contents'])} objects in the folder")

            # Loop through each object in the folder
            for obj in response['Contents']:
                object_key = obj['Key']
            
                # Only process JSON files
                if object_key.lower().endswith('.json'):
                    logger.info(f"\nProcessing JSON file: {object_key}")
                    
                    try:
                        # Read the JSON file from S3
                        file_response = s3.get_object(Bucket=s3_bucket, Key=object_key)
                        data = json.loads(file_response["Body"].read().decode("utf-8"))

                        logger.info("Data:")
                        logger.info(json.dumps(data, indent=2))

                        # Send POST request for each file
                        response = requests.post(api_url, json=data)
                        logger.info("Status Code: %s", response.status_code)
                        logger.info("Response Body: %s", response.text)

                        return {
                            "statusCode": 200,
                            "s3_path": s3_path,
                            "source_bucket": source_bucket,
                            "source_key": source_key,
                            "s3_bucket": s3_bucket,
                            "newFileName": new_file_name,
                            "classification": classification,
                            "body": json.dumps({
                                "message": f"Successfully processed {object_key}",
                                "status_code": response.status_code,
                                "response_body": response.text
                            })
                        }

                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON from {object_key}: {e}")
                    except Exception as e:
                        logger.error(f"Error processing {object_key}: {e}")
                else:
                    logger.info(f"Skipping non-JSON file: {object_key}")

    except Exception as e:
        logger.error(f"Error processing S3 objects: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    


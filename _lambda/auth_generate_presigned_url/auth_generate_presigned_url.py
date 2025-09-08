import json
import os
import uuid
import boto3
from jwt import decode as jwt_decode, InvalidTokenError
from boto3 import client as boto3_client
from botocore.client import Config
from datetime import datetime

# Initialize secret store manager outside handler for reuse
ssm_client = boto3.client('ssm', region_name=os.environ.get('AWS_REGION'))

#region_name = os.environ.get('AWS_REGION')

# Initialize S3 client outside handler for reuse
s3_client = boto3_client(
    's3',
    region_name=os.environ.get('AWS_REGION', 'ap-southeast-2'),
    config=Config(signature_version='s3v4')
)


# Get the bucket name from environment variables
bucket_name = os.environ['BUCKET_NAME']
upload_prefix = os.environ['UPLOAD_PREFIX']
parameter_name = os.environ['PARAMETER_NAME']


# Get JWT secret from secret store manager
try:
    response = ssm_client.get_parameter(
        Name=parameter_name, 
        WithDecryption=True
    )
    JWT_SECRET = response['Parameter']['Value']
    
except ssm_client.exceptions.ParameterNotFound:
    print("Error: Parameter not found.")
except Exception as e:
    print(f"An error occurred: {e}")


def handler(event, context):
    """
    Lambda function to authenticate the jwt token and if valid, generate a presigned URL for S3 file uploads
    """
    headers = event.get('headers', {})
    query_params = event.get('queryStringParameters', {}) or {}
    
    # Step 1: Check if JWT token is provided or valid token
    
    token = headers.get('authorization')
    if not token or not token.startswith("Bearer "):
         return {"statusCode": 401, "body": "Missing or invalid Authorization header"}

    try:
        decoded = jwt_decode(token.replace("Bearer ", ""), JWT_SECRET, algorithms=["HS256"])
        topic_name = decoded.get('topic_name')
        source_name = decoded.get('source_name')

        if not topic_name or not source_name:
            return {"statusCode": 500, "body": "Failed to decode JWT token"}

    except InvalidTokenError as e:
        return {"statusCode": 403, "body": f"Invalid JWT: {str(e)}"}
    
    
    # Step 2: Validate file format and set content type
    original_filename = query_params.get('originalFileName', '')
    
    if not original_filename:
        return {"statusCode": 400, "body": "Missing originalFileName parameter"}
    
    # Get file extension
    file_extension = original_filename.lower().split('.')[-1] if '.' in original_filename else ''
    
    # Define allowed formats and their content types
    allowed_formats = {
        'pdf': 'application/pdf',
        'png': 'image/png', 
        'gif': 'image/gif',
        'jpg': 'image/jpeg',
        'jpeg': 'image/tiff'
    }
    
    # Validate file format
    if file_extension not in allowed_formats:
        return {
            "statusCode": 400, 
            "body": f"File format '{file_extension}' not allowed. Only PDF, PNG, GIF, JPEG, and TIFF files are supported."
        }
    
    content_type = allowed_formats[file_extension]


    try:

        

        # Generate a presigned URL for PUT operation
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': f"{upload_prefix}/{original_filename}",
                'ContentType': content_type
            },
            ExpiresIn=3600,  # URL valid for 1 hour
            HttpMethod='PUT'
        )

        # Consistent CORS headers
        cors_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'PUT,OPTIONS,GET'
        }
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'presigned_url': presigned_url,
                'file_name': original_filename,
                'content_type': content_type
            })

        }

    except Exception as e:
        import traceback
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'PUT,OPTIONS,GET'
            },
            'body': json.dumps({
                'error': str(e)
            })
        }


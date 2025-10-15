#!/usr/bin/env python3
"""
Script to validate Textract Adapter training data
Checks if all referenced annotation files exist in S3
"""

import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def validate_training_data(validation_file_path):
    """
    Validate training data by checking if annotation files exist
    """
    s3_client = boto3.client('s3')
    
    missing_files = []
    total_files = 0
    
    with open(validation_file_path, 'r') as f:
        for line in f:
            if line.strip():
                data = json.loads(line.strip())
                total_files += 1
                
                # Extract S3 path from annotations-ref
                annotations_ref = data.get('annotations-ref', '')
                if annotations_ref.startswith('s3://'):
                    # Parse S3 path
                    s3_path = annotations_ref.replace('s3://', '')
                    bucket_name = s3_path.split('/')[0]
                    object_key = '/'.join(s3_path.split('/')[1:])
                    
                    try:
                        # Check if object exists
                        s3_client.head_object(Bucket=bucket_name, Key=object_key)
                        print(f"✓ Found: {annotations_ref}")
                    except ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            missing_files.append(annotations_ref)
                            print(f"✗ Missing: {annotations_ref}")
                        else:
                            print(f"⚠ Error checking {annotations_ref}: {e}")
    
    print(f"\n=== VALIDATION SUMMARY ===")
    print(f"Total files checked: {total_files}")
    print(f"Missing files: {len(missing_files)}")
    print(f"Success rate: {((total_files - len(missing_files)) / total_files * 100):.1f}%")
    
    if missing_files:
        print(f"\n=== MISSING FILES ===")
        for file in missing_files[:10]:  # Show first 10
            print(f"- {file}")
        if len(missing_files) > 10:
            print(f"... and {len(missing_files) - 10} more")
    
    return len(missing_files) == 0

if __name__ == "__main__":
    # You would run this with your validation error file
    # validate_training_data('validation_error.jsonl')
    print("Validation script created. Update the file path and run to check your training data.")
#!/usr/bin/env python3
import boto3
import json
import os
from dotenv import load_dotenv
import os

load_dotenv()

# Read from .env files
secret_name = os.getenv('SECRET_NAME')
parameter_name = os.getenv('PARAMETER_NAME')
profile = "laddprofile"
session = boto3.Session(profile_name=profile)
region = session.region_name

def create_ssm_parameter():
        
    # Create SSM client
    ssm = boto3.client('ssm', region_name=region)
    
    try:
        # Try to create the parameter
        response = ssm.put_parameter(
            Name=parameter_name,
            Value=secret_name,
            Type='SecureString',
            Description='JWT token secret for Hepburn Topic accounts payable',
            Overwrite=True
        )
        print(f"Successfully created/updated parameter: {parameter_name}")
        return True
    except Exception as e:
        print(f"Error creating parameter: {e}")
        return False

if __name__ == "__main__":
    create_ssm_parameter()
    
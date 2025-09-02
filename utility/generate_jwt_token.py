import jwt
import boto3
import json
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()

config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)).replace('utility', ''), 'config.json')

with open(config_path, 'r') as f:
    config = json.load(f)

topic_name = config.get('topic_name')
source_name = config.get('source_name')
algorithm = "HS256" 
profile = "laddprofile"
parameter_name = os.getenv('PARAMETER_NAME')
jwttoken = config.get('jwttoken')

session = boto3.Session(profile_name=profile)
region = session.region_name

# Create SSM client for Parameter Store
client = boto3.client('ssm', region_name=region)

# Create client for secret manager
secret_client = boto3.client('secretsmanager', region_name=region)

# Store JWT token in SSM Parameter Store
def store_jwt_token(token, token_name):
    try:
        secret_client.put_secret_value(SecretId=token_name, SecretString=token)
    except secret_client.exceptions.ResourceNotFoundException:
        secret_client.create_secret(Name=token_name, SecretString=token)

# Generate JWT
def generate_jwt(topic_name, source_name, secret_name, store_token=True):
    
    payload = {
        "topic_name": topic_name,
        "source_name": source_name,
    }

    token = jwt.encode(payload, secret_name, algorithm="HS256")

    if store_token:
        token_name = f"{topic_name}_jwt_token"
        store_jwt_token(token, token_name)

    return token

# Retrieve JWT secret from SSM Parameter Store
def getSecretName(paramname):
    response = client.get_parameter(
        Name=paramname,
        WithDecryption=True
    )
    return response['Parameter']['Value']

# Decode JWT token to get topic_name and source_name
def decode_jwt_token(token, secret):
    decoded = jwt.decode(token, secret, algorithms=["HS256"])
    return decoded.get("topic_name"), decoded.get("source_name")

if __name__ == "__main__":
    # Get Secret Name
    secret_name = getSecretName(parameter_name)
    

    # Generate and print JWT token
    # token = generate_jwt(topic_name, source_name, secret_name)
    # print("Generated JWT Token:")
    # print(token)

    # Retrieve and print JWT token from SSM Parameter Store
    # topic_name, source_name = decode_jwt_token(jwttoken, secret_name)
    # print(topic_name, source_name)
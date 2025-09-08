# JWT Lambda Layer

This layer provides the PyJWT library for Lambda functions that need JWT token authentication.

## Contents
- PyJWT 2.8.0

## Usage
1. Build the layer: `python build_layer.py`
2. The layer is automatically included in the DocumentLakeIngestionStack
3. Lambda functions can import: `from jwt import decode, InvalidTokenError`

## Deployment
The layer is built and deployed automatically when using `python build_and_deploy.py`
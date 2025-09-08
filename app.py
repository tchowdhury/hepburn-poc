#!/usr/bin/env python3
import os
import aws_cdk as cdk
import json
from hepburn.hepburn_stack import HepburnStack

config_path = os.path.join(os.path.dirname(__file__), 'config.json')

with open(config_path, 'r') as f:
    config = json.load(f)
    account = config.get('aws_account_id')
    region = config.get('aws_region')

app = cdk.App()
HepburnStack(app, "HepburnStack",
    env=cdk.Environment(
        account=account,
        region=region
    )
)

app.synth()
import os
import io
import json
import boto3
import botocore
import pickle
import requests
import traceback
import zipfile
import re
import shutil
import yaml
import time
import hashlib
import uuid
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64
from typing import Tuple
import redis
import pandas as pd
from dotenv import load_dotenv
from src.multithreading import MultiThreading


DEFAULT_ENV = "dev"
env = os.getenv('APP_ENV', DEFAULT_ENV)
cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=840, connect_timeout=600, region_name="us-east-1")
lambdaClient = boto3.client('lambda', config=cfg, region_name="us-east-1")
s3Client = boto3.client("s3")
# Create an SQS client
sqs = boto3.client('sqs', region_name="us-east-1")  
CONFIG_PATH = f"./config_{env}.yml"

def load_config(config_path):
    config = {}
    with open(config_path) as f:
        config =  yaml.safe_load(f)

    if not config:
        raise Exception("Issue in loading the configuration file")

    return config

def lambda_handler(event, context):
    print(f"Detected Environment: {env}")
    print(f"Event: {event}")
    start = time.time()

    bucket, file_key = fetch_bucket_user_id_report_id(event)

    print(bucket, file_key)

    end = time.time()
    print("Total Time in Seconds: " + str(end - start))
    return {
        'statusCode': 200,
        'body': 'Hello from Dockerized Lambda!'
    }


def fetch_bucket_user_id_report_id(event: list):
    # Extract the records from the event
    records = event.get('Records', [])

    config_file = load_config(CONFIG_PATH)
    # files_testing_buckets = config_file.get('files_testing', '')
    # temp_file_bucket = config_file.get('temp_file_s3_bucket', '')

    for record in records:
        body = json.loads(record['body'])
        
        file_key = body.get('file_key')
        bucket = body.get('bucket')

        return bucket, file_key

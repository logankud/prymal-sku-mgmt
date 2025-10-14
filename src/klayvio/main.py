import requests
import json 
import pandas as pd
import time
import os
import boto3
from utils import list_all_campaigns

# import from src/utils (not src/klayvio/utils)
import sys
sys.path.append('src/')
from utils import write_list_of_dicts_to_s3


headers = {
        "Content-Type": "application/json",
         "accept": "application/vnd.api+json",
        "Authorization": f"Klaviyo-API-Key {os.getenv('KLAYVIO_API_KEY')}",
        "revision": "2025-07-15"
    }

# List all campaigns
all_campaigns = list_all_campaigns(request_headers=headers)

# Configure s3 path
today = pd.to_datetime('today').strftime('%Y-%m-%d')
s3_path = f"klayvio/campaigns/load_dt={today}/campaigns_{today}.json"

# Instantiate s3 client
s3_client = boto3.client('s3', region_name='us-east-1', aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), aws_secret_access_key=os.getenv('AWS_ACCESS_SECRET'))

# Write to s3
write_list_of_dicts_to_s3(bucket=os.getenv('S3_BUCKET_NAME'), 
          key=s3_path, 
          list_of_dicts=all_campaigns, 
          s3_client=s3_client)


import boto3
import os

import sys

sys.path.append('src/')  # updating path back to root for importing modules

from utils import *
from models import *


# Fetch AWS credentials from environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_ACCESS_SECRET')


# Get Shipbob API secret
shipbob_api_secret = os.getenv('SHIPBOB_API_SECRET')


product_to_inventory_df = list_all_shipbob_products(shipbob_api_secret)

product_to_inventory_df.to_csv('products_to_inventory.csv',index=False)

print(product_to_inventory_df.loc[product_to_inventory_df['product_id'] ==24550])
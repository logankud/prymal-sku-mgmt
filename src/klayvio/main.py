import requests
import json 
import pandas as pd
import time
import os
from utils import list_all_campaigns

# import from src/utils (not src/klayvio/utils)



KLAYVIO_ORG = os.getenv('KLAYVIO_ORG')
KLAYVIO_API_KEY = os.getenv('KLAYVIO_API_KEY')

headers = {
        "Content-Type": "application/json",
         "accept": "application/vnd.api+json",
        "Authorization": f"Klaviyo-API-Key {KLAYVIO_API_KEY}",
        "revision": "2025-07-15"
    }



all_campaigns = list_all_campaigns(request_headers=headers)


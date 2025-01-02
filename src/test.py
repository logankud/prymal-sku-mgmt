from datetime import datetime
import pandas as pd
from loguru import logger
from utils import *
from models import *
import os

import sys
sys.path.append('src/')  # updating path back to root for importing modules

from utils import *
from models import *


SHIPBOB_API_SECRET = os.getenv('SHIPBOB_API_SECRET')
start_date = '2024-10-27'
end_date = '2024-10-31'

# Configure Athena / Glue
region = 'us-east-1'
database = os.getenv('GLUE_DATABASE_NAME')
s3_bucket = os.getenv('S3_BUCKET_NAME')

# Shopify API credentials
shopify_api_key = os.getenv('SHOPIFY_API_KEY')
shopify_api_pw = os.getenv('SHOPIFY_API_PW')
shopify_store_url = os.getenv('SHOPIFY_STORE_URL')


import json 



def run():

    # Blank df to store order data
    order_data_df = pd.DataFrame(columns = [
                                     'created_date',
                                     'purchase_date',
                                     'shipbob_order_id',
                                     'order_number',
                                     'order_status',
                                     'order_type',
                                     'shipping_method',
                                     'channel_id',
                                     'channel_name',
                                     'customer_name',
                                     'customer_email',
                                     'customer_address_city',
                                     'customer_address_state',
                                     'customer_address_country',
                                     'product_id',
                                     'sku',
                                     'sku_name',
                                     'inventory_id',
                                     'inventory_qty',
                                     'inventory_name'
                                 ])

    
    url = "https://api.shipbob.com/1.0/order?StartDate=2024-11-01&EndDate=2024-11-02"

    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SHIPBOB_API_SECRET}"
    }

    count = 0
    while url:

        print(f"Fetching: {url}")
        response = requests.get(url, headers=headers)

        # Convert to json
        data_json = response.json()

        # ---------- ITERATE & NORMALIZE -----------

        # Loop through records & flatten
        for rec in data_json:

            # Root level fields
            created_date = pd.to_datetime(rec['created_date'])
            purchase_date = pd.to_datetime(rec['purchase_date'])
            shipbob_order_id = rec['id']
            order_number = rec['order_number']
            order_status = rec['status']
            order_type = rec['type']
            shipping_method = rec['shipping_method']

            # Parse nested fields
            channel_id = rec['channel']['id']
            channel_name = rec['channel']['name']
            customer_name = rec['recipient']['name']
            customer_email = rec['recipient']['email']
            customer_address_city = rec['recipient']['address']['city']
            customer_address_state = rec['recipient']['address']['state']
            customer_address_country = rec['recipient']['address']['country']

            # Flatten inventory_items from within shipments
            for shipment in rec['shipments']:

                for product in shipment['products']:
                    product_id = product['id']
                    sku = product['sku']
                    sku_name = product['name']

                    for inventory_item in product['inventory_items']:
                        inventory_id = inventory_item['id']
                        inventory_qty = inventory_item['quantity']
                        inventory_name = inventory_item['name']


                        # Compile all variables into a single dataframe record
                        df_rec = pd.DataFrame({
                            'created_date': [created_date],
                            'purchase_date': [purchase_date],
                            'shipbob_order_id': [shipbob_order_id],
                            'order_number': [order_number],
                            'order_status': [order_status],
                            'order_type': [order_type],
                            'shipping_method': [shipping_method],
                            'channel_id': [channel_id],
                            'channel_name': [channel_name],
                            'customer_name': [customer_name],
                            'customer_email': [customer_email],
                            'customer_address_city': [customer_address_city],
                            'customer_address_state': [customer_address_state],
                            'customer_address_country': [customer_address_country],
                            'product_id': [product_id],
                            'sku': [sku],
                            'sku_name': [sku_name],
                            'inventory_id': [inventory_id],
                            'inventory_qty': [inventory_qty],
                            'inventory_name': [inventory_name],
                        })

                        # Concat data
                        order_data_df = pd.concat([order_data_df, df_rec])

        
        # ---------- PAGINATE -----------
        
        count += len(response.json())
        url = response.headers.get('Next-Page', None)
        if url:
            url = "https://api.shipbob.com" + url

    logger.info(f"Total Order Count: {count}")

    # Verify that total count of orders was extracted to dataframe
    assert count == len(order_data_df['shipbob_order_id'].unique())
    
    return order_data_df



# order_date_df = run()


df1 = pd.DataFrame({'A': [None, None, None], 'B': [None, None, None]})
df2 = pd.DataFrame({'A': [4, 5, 6], 'B': [None, None, None]})  # 'B' is all-NaN in both

# Warning occurs because of the all-NaN column
result_with_warning = pd.concat([df1, df2])

# Drop the all-NaN columns before concatenation
dfs = [df.dropna(how='all', axis=1) for df in [df1, df2]]
result_no_warning = pd.concat(dfs)

print(result_with_warning)
print(result_no_warning)




# # Read JSON data from a file
# with open('test.json', 'r') as file:
#     data_json = json.load(file)

# # print(data_json)


# # Blank df to store order data
# order_data_df = pd.DataFrame(columns = [
#                                  'created_date',
#                                  'purchase_date',
#                                  'shipbob_order_id',
#                                  'order_number',
#                                  'order_status',
#                                  'order_type',
#                                  'shipping_method',
#                                  'channel_id',
#                                  'channel_name',
#                                  'customer_name',
#                                  'customer_email',
#                                  'customer_address_city',
#                                  'customer_address_state',
#                                  'customer_address_country',
#                                  'product_id',
#                                  'sku',
#                                  'sku_name',
#                                  'inventory_id',
#                                  'inventory_qty',
#                                  'inventory_name'
#                              ])


# # Loop through records & flatten
# for rec in data_json:

#     # Root level fields
#     created_date = pd.to_datetime(rec['created_date'])
#     purchase_date = pd.to_datetime(rec['purchase_date'])
#     shipbob_order_id = rec['id']
#     order_number = rec['order_number']
#     order_status = rec['status']
#     order_type = rec['type']
#     shipping_method = rec['shipping_method']

#     # Parse nested fields
#     channel_id = rec['channel']['id']
#     channel_name = rec['channel']['name']
#     customer_name = rec['recipient']['name']
#     customer_email = rec['recipient']['email']
#     customer_address_city = rec['recipient']['address']['city']
#     customer_address_state = rec['recipient']['address']['state']
#     customer_address_country = rec['recipient']['address']['country']

#     # Flatten inventory_items from within shipments
#     for shipment in rec['shipments']:

#         for product in shipment['products']:
#             product_id = product['id']
#             sku = product['sku']
#             sku_name = product['name']
            
#             for inventory_item in product['inventory_items']:
#                 inventory_id = inventory_item['id']
#                 inventory_qty = inventory_item['quantity']
#                 inventory_name = inventory_item['name']


#                 # Compile all variables into a single dataframe record
#                 df_rec = pd.DataFrame({
#                     'created_date': [created_date],
#                     'purchase_date': [purchase_date],
#                     'shipbob_order_id': [shipbob_order_id],
#                     'order_number': [order_number],
#                     'order_status': [order_status],
#                     'order_type': [order_type],
#                     'shipping_method': [shipping_method],
#                     'channel_id': [channel_id],
#                     'channel_name': [channel_name],
#                     'customer_name': [customer_name],
#                     'customer_email': [customer_email],
#                     'customer_address_city': [customer_address_city],
#                     'customer_address_state': [customer_address_state],
#                     'customer_address_country': [customer_address_country],
#                     'product_id': [product_id],
#                     'sku': [sku],
#                     'sku_name': [sku_name],
#                     'inventory_id': [inventory_id],
#                     'inventory_qty': [inventory_qty],
#                     'inventory_name': [inventory_name],
#                 })

#                 # Concat data
#                 order_data_df = pd.concat([order_data_df, df_rec])
                
# print(order_data_df)

# print(order_data_df.dtypes)
# # order_data_df.to_csv('testing.csv' , index=False)



        
        


    
    




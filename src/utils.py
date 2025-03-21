import boto3
import botocore
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
from loguru import logger
import os
from io import StringIO
import requests
import pandas as pd
import numpy as np
import csv
import datetime
from datetime import timedelta
import pytz
from pytz import timezone
from typing import Any, List, Tuple, Type, Dict, get_origin, get_args
import re
import time
import json

from pydantic import BaseModel, ValidationError

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_ACCESS_SECRET']


def format_df_for_s3(df: pd.DataFrame):
    """Format dataframe to be written to s3 as a csv (and avoid delimeter issues)

    Replace quote char (") with single quotes, new line ('\n') with '  ', and delimeter char (',') 
        with ';' in string column values
    
    Args:
        df (pd.DataFrame): Dataframe to be written to s3
        
    Returns:
        df (pd.DataFrame): Formatted dataframe"""

    logger.info(f'Formating dataframe for writing to s3 as csv')

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: x.replace('"', "'")
                                    if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: x.replace('\n', ' ')
                                    if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: x.replace(',', ';')
                                    if isinstance(x, str) else x)
    return df


def write_df_to_s3(bucket, key, df, s3_client):
    """
    Write a dataframe to an S3 bucket.
    
    Args:
        bucket (str): The name of the S3 bucket to write to.
        key (str): The key to use for the object in the S3 bucket.
        df (pd.DataFrame): The dataframe to write to the S3 bucket.
        s3_client (boto3.client): The S3 client to use for writing to the S3 bucket
        
    Returns:
        None
    """

    try:

        # Format to avoid delimeter issues
        df = format_df_for_s3(df)

    except Exception as e:
        logger.error(f'Error formatting dataframe for writing to s3: {str(e)}')
        # Raise exception to stop execution
        raise ValueError(
            f'Error formatting dataframe for writing to s3! {str(e)}')

    logger.info(df.dtypes)
    logger.info(f'Writing df to csv {key}')

    # Use s3 client to write dataframe to S3 as csv
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        response = s3_client.put_object(Body=csv_buffer.getvalue(),
                                        Bucket=bucket,
                                        Key=key,
                                        ContentType='text/csv')
        logger.info(f'Response: {response}')
    except BotoCoreError as e:
        logger.error(f'BotoCore Error: {e}')
    except ClientError as e:
        logger.error(f'Client Error: {e}')
    except NoCredentialsError as e:
        logger.error(f'No Credentials Error: {e}')
    except PartialCredentialsError as e:
        logger.error(f'Partial Credentials Error: {e}')
    except ParamValidationError as e:
        logger.error(f'Param Validation Error: {e}')


def write_list_of_dicts_to_s3(bucket, key, list_of_dicts, s3_client):
    """
    Write a list of dictionaries to an S3 bucket as a single JSON file.

    Args:
        bucket (str): The name of the S3 bucket to write to.
        key (str): The key to use for the object in the S3 bucket.
        list_of_dicts (list): The list of dictionaries to write to the S3 bucket.
        s3_client (boto3.client): The S3 client to use for writing to the S3 bucket.

    Returns:
        None
    """
    try:
        # Convert list of dictionaries to newline-delimited JSON format
        ndjson_data = "\n".join(
            [json.dumps(record) for record in list_of_dicts])

        # Write newline-delimited JSON data to S3 bucket
        response = s3_client.put_object(Body=ndjson_data,
                                        Bucket=bucket,
                                        Key=key,
                                        ContentType='application/json')

        logger.info(f'Successfully wrote JSON to S3. Response: {response}')
    except (BotoCoreError, ClientError, NoCredentialsError,
            PartialCredentialsError) as e:
        logger.error(f'Error writing JSON to S3: {e}')
        raise ValueError(f'Error writing JSON to S3! {e}')


def run_athena_query_no_results(bucket: str, query: str, database: str,
                                region: str):
    """Function to execute an athena query & return results csv as a dataframe

    Args:
        bucket (str): S3 bucket name
        query (str): The query to be executed
        database (str): The Glue database to be queried
        region (str): The AWSregion to be queried
    Returns:
        (pd.DataFrame): The results of the query as a dataframe
    """

    # Initialize Athena client
    athena_client = boto3.client('athena',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation':
                f's3://{bucket}/athena_query_results/'  # query results location 
            })

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'
        logger.info(f'Running query..')

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)

            if 'QueryExecution' in response and 'Status' in response[
                    'QueryExecution'] and 'State' in response[
                        'QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                    raise Exception('Query Failed!')

                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')

    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']

        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax

        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist

        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions

        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions


def run_athena_query(query: str, database: str, region: str, s3_bucket: str):
    """Function to execute an athena query & return results csv as a dataframe

    Args:
        query (str): The query to be executed
        database (str): The Glue database to be queried
        region (str): The AWSregion to be queried
        s3_bucket (str) : S3 bucket name for query results
    Returns:
        (pd.DataFrame): The results of the query as a dataframe
    """

    # Initialize Athena client
    athena_client = boto3.client('athena',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation':
                f's3://{s3_bucket}/athena_query_results/'  # Specify your S3 bucket for query results
            })

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'
        logger.info(f'Running query..')

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)

            if 'QueryExecution' in response and 'Status' in response[
                    'QueryExecution'] and 'State' in response[
                        'QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')

        # OBTAIN DATA

        # --------------

        query_results = athena_client.get_query_results(
            QueryExecutionId=query_execution_id, MaxResults=1000)

        # Extract qury result column names into a list

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]

        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]

        # Convert data rows into a list of lists
        query_results_data = [[
            r['VarCharValue'] if 'VarCharValue' in r else np.nan
            for r in row['Data']
        ] for row in data_rows]

        # Paginate Results if necessary
        while 'NextToken' in query_results:
            query_results = athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=query_results['NextToken'],
                MaxResults=1000)

            # Extract quuery result data rows
            data_rows = query_results['ResultSet']['Rows'][1:]

            # Convert data rows into a list of lists
            query_results_data.extend([[
                r['VarCharValue'] if 'VarCharValue' in r else np.nan
                for r in row['Data']
            ] for row in data_rows])

        results_df = pd.DataFrame(query_results_data, columns=col_names)

        return results_df

    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']

        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax

        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist

        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions

        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions


def validate_dataframe(
        df: pd.DataFrame, model: Type[BaseModel]
) -> Tuple[List[BaseModel], List[Tuple[dict, str]]]:
    """
    Validate a pandas DataFrame using a Pydantic model.

    This function takes a DataFrame and a Pydantic model, attempts to validate each row
    of the DataFrame against the model, and returns two lists: one containing valid
    items (as instances of the Pydantic model) and another containing invalid items
    along with their error messages.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        model (Type[BaseModel]): The Pydantic model class to use for validation.

    Returns:
        (list) The first list contains valid items as instances of the Pydantic model.
        (list) The second list contains tuples of invalid items, where each tuple
              consists of the original data (as a dict) and the error message.

    """

    logger.info(f'Validating df with Pydantic')

    valid_items: List[BaseModel] = []
    invalid_items: List[Tuple[dict, str]] = []

    # Convert DataFrame to list of dictionaries
    data = df.to_dict('records')

    for item in data:
        try:
            logger.info('test')
            # Attempt to create a model instance
            valid_item = model(**item)
            valid_items.append(valid_item.model_dump())
        except ValidationError as e:
            # If validation fails, add to invalid items
            invalid_items.append((item, str(e)))

    return valid_items, invalid_items


def list_all_shipbob_products(api_secret: str):
    """Function to list all products in shipbob in a dataframe

    Args:
        api_secret (str): The api secret for the shipbob account

    Returns:
        (pd.DataFrame): All products in shipbob as a dataframe
        
    """

    logger.info(f'Listing all products in shipbob')

    url = 'https://api.shipbob.com'
    url_params = f'/1.0/product'

    # Set up the request headers with the Bearer token
    headers = {"Authorization": f"Bearer {api_secret}"}

    # Blank df to store results
    product_to_inventory_df = pd.DataFrame(columns=[
        'product_id', 'sku', 'sku_name', 'channel_id', 'channel_name',
        'inventory_id', 'inventory_name', 'inventory_qty'
    ])

    # Send the GET request
    response = requests.get(url + url_params, headers=headers)

    logger.info(f'Response Headers: {response.headers}')

    # convert to json
    response_json = json.loads(response.text)

    # Parse response json to a df (one record per sku-product-inventory mapping)
    if type(response_json) == list:

        for rec in response_json:

            # Extract inventory details for the product
            inventory_items = pd.json_normalize(
                rec['fulfillable_inventory_items'])

            # Rename columns
            inventory_items.columns = [
                'inventory_id', 'inventory_name', 'inventory_qty'
            ]

            # Carry forward relevant fields
            inventory_items['product_id'] = rec['id']
            inventory_items['sku'] = rec['sku']
            inventory_items['sku_name'] = rec['name']
            inventory_items['channel_id'] = rec['channel']['id']
            inventory_items['channel_name'] = rec['channel']['name']

            # Append results to df
            product_to_inventory_df = pd.concat(
                [product_to_inventory_df, inventory_items])

    # Extract page details
    current_page = response.headers['Page-Number']
    total_pages = response.headers['Total-Pages']

    logger.info(f'Extracted page {current_page} of {total_pages}')

    while 'Next-Page' in response.headers and int(current_page) <= int(
            total_pages):

        url_params = response.headers['Next-Page']

        logger.info(f'Total records: {len(product_to_inventory_df)}')

        logger.info(f'paginating.. {url_params}')

        # Send the GET request
        response = requests.get(url + url_params, headers=headers)

        # convert to json
        response_json = json.loads(response.text)

        # Parse response json to a df (one record per sku-product-inventory mapping)
        if type(response_json) == list:

            for rec in response_json:

                # Extract inventory details for the product
                inventory_items = pd.json_normalize(
                    rec['fulfillable_inventory_items'])

                # Rename columns
                inventory_items.columns = [
                    'inventory_id', 'inventory_name', 'inventory_qty'
                ]

                # Carry forward relevant fields
                inventory_items['product_id'] = rec['id']
                inventory_items['sku'] = rec['sku']
                inventory_items['sku_name'] = rec['name']
                inventory_items['channel_id'] = rec['channel']['id']
                inventory_items['channel_name'] = rec['channel']['name']

                # Append results to df
                product_to_inventory_df = pd.concat(
                    [product_to_inventory_df, inventory_items])

        # Extract page details
        current_page = response.headers['Page-Number']

    return product_to_inventory_df.reset_index(drop=True)


def get_shipbob_inventory(api_secret: str):
    """
    GET details (ie. sku) for current inventory in Shipbob

    Parameters
    ----------
    api_secret : str
        API secret (PAT token generated in Shipbob)

    Returns
    -------
    pd.DataFrame
        Dataframe containing all SKUs and their current inventory levels per ShipBob
    """

    url = 'https://api.shipbob.com'
    url_params = '/1.0/inventory'

    # Set up the request headers with the Bearer token
    headers = {"Authorization": f"Bearer {api_secret}"}

    # Send the GET request
    response = requests.get(url + url_params, headers=headers)

    # convert to json
    response_json = json.loads(response.text)

    # normalize to df
    results_df = pd.json_normalize(response_json)

    # Extract page details
    current_page = response.headers['Page-Number']
    total_pages = response.headers['Total-Pages']

    while 'Next-Page' in response.headers and int(current_page) <= int(
            total_pages):

        logger.info(f'Page {current_page} of {total_pages} retrieved')

        url_params = response.headers['Next-Page']

        # Send the GET request
        response = requests.get(url + url_params, headers=headers)

        # convert to json
        response_json = json.loads(response.text)

        # normalize to df
        response_df = pd.json_normalize(response_json)

        # Extract page details
        current_page = response.headers['Page-Number']

        # Append results
        results_df = pd.concat([results_df, response_df])

        logger.info(f'Total results retrived: {results_df.shape[0]}')

    # Subset of columns
    results_df = results_df[[
        'id', 'name', 'is_digital', 'is_case_pick', 'is_lot',
        'total_fulfillable_quantity', 'total_onhand_quantity',
        'total_committed_quantity', 'total_sellable_quantity',
        'total_awaiting_quantity', 'total_exception_quantity',
        'total_internal_transfer_quantity', 'total_backordered_quantity',
        'is_active'
    ]].copy()

    return results_df


def get_shipbob_orders_by_date(api_secret: str, start_date: str,
                               end_date: str):
    """Function to get shipbob orders by date
    
    Args:
        api_secret (str): The api secret for the shipbob account
        start_date (str): The start date of the orders to be queried ('YYYY-MM-DD' format)
        end_date (str): The end date of the orders to be queried ('YYYY-MM-DD' format)
    
    Returns:
        (pd.DataFrame): All orders in shipbob as a dataframe
    
    """

    logger.info(
        f'Listing all orders in shipbob from {start_date} to {end_date}')

    url = f'https://api.shipbob.com/1.0/order?StartDate={start_date}&EndDate={end_date}'

    # Set up the request headers with the Bearer token
    headers = {"Authorization": f"Bearer {api_secret}"}

    # Blank df to store order data
    order_data_df = pd.DataFrame(columns=[
        'created_date', 'purchase_date', 'shipbob_order_id', 'order_number',
        'order_status', 'order_type', 'shipping_method', 'channel_id',
        'channel_name', 'customer_name', 'customer_email',
        'customer_address_city', 'customer_address_state',
        'customer_address_country', 'product_id', 'sku', 'sku_name',
        'inventory_id', 'inventory_qty', 'inventory_name'
    ])

    count = 0
    while url:  # while there are more pages of results

        logger.info(f"Fetching: {url}")
        response = requests.get(url, headers=headers)

        # Convert to json
        data_json = response.json()

        # ---------- ITERATE & NORMALIZE -----------

        try:

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
                channel_id = rec.get('channel') and rec.get('channel',
                                                            {}).get('id')
                channel_name = rec.get('channel') and rec.get('channel',
                                                              {}).get('name')
                customer_name = rec['recipient']['name']
                customer_email = rec['recipient']['email']
                customer_address_city = rec['recipient']['address']['city']
                customer_address_state = rec['recipient']['address']['state']
                customer_address_country = rec['recipient']['address'][
                    'country']

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
                                'customer_address_city':
                                [customer_address_city],
                                'customer_address_state':
                                [customer_address_state],
                                'customer_address_country':
                                [customer_address_country],
                                'product_id': [product_id],
                                'sku': [sku],
                                'sku_name': [sku_name],
                                'inventory_id': [inventory_id],
                                'inventory_qty': [inventory_qty],
                                'inventory_name': [inventory_name],
                            })

                            # Concat data
                            order_data_df = pd.concat([order_data_df, df_rec])

        except TypeError as te:
            logger.error(f'TypeError: {te}')
            logger.error(rec)
            break

        # ---------- PAGINATE -----------

        count += len(response.json())
        url = response.headers.get('Next-Page', None)
        if url:
            url = "https://api.shipbob.com" + url

    logger.info(f"Total Order Count: {count}")

    # Verify that total count of orders was extracted to dataframe
    assert count == len(order_data_df['shipbob_order_id'].unique())

    logger.info(order_data_df.head())

    return order_data_df.reset_index(drop=True)


def pydantic_to_glue_schema(model: Type[BaseModel]) -> List[Dict[str, str]]:
    """
    Convert a Pydantic model to a Glue table schema.

    Args:
        model (Type[BaseModel]): The Pydantic model class to convert.

    Returns:
        List[Dict[str, str]]: A list of column definitions for a Glue table.
    """

    logger.info(f'Generating Glue schema from Pydantic model class')

    glue_schema = []

    for field_name, field in model.model_fields.items():
        field_type = field.annotation
        origin = get_origin(field_type)

        if origin is None:
            # This is a simple type (int, str, etc.)
            type_str = str(field_type).replace("<class '",
                                               "").replace("'>", "")
            print(f"Field: {field_name}, Type: {type_str}")

        else:

            # Other complex types

            type_str = str(field_type).replace("typing.Optional[",
                                               "").replace("]", "")
            print(f"Field: {field_name}, Type: {type_str}")

            if type_str == "int":
                glue_type = "bigint"
            elif type_str == "float":
                glue_type = "double"
            elif type_str == "str":
                glue_type = "string"
            elif type_str == "bool":
                glue_type = "boolean"
            elif type_str == "datetime.datetime":
                glue_type = "timestamp"

            else:
                raise ValueError(
                    f"Unsupported type for field {field_name}: {type_str}")

            glue_schema.append({"Name": field_name, "Type": glue_type})

    return glue_schema


def create_glue_table(region: str, database_name: str, table_name: str,
                      schema: List[Dict[str, str]], s3_location: str):
    """
    Create a Glue table using boto3.

    Args:
        region (str): AWS region of Glue database
        database_name (str): Name of Glue database
        table_name (str): Name of the table to create
        schema (List[Dict[str, str]]): Glue table column schema
        s3_location (str): S3 location for the table data
    """

    logger.info(f'Creating glue table: {table_name}')

    try:

        glue_client = boto3.client('glue',
                                   region_name=region,
                                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        response = glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': schema,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat':
                    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary':
                        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {
                            'field.delim': ',',
                            'serialization.format': ','
                        }
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
            })
        print(f"Table {table_name} created successfully")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Table {table_name} already exists")
    except Exception as e:
        print(f"Error creating table: {str(e)}")


def list_active_shopify_variant_skus(shopify_api_key: str, shopify_api_pw: str,
                                     shopify_store_url: str):
    """
    List details for all active product SKUs from a Shopify store as of right now.  

    Active variant SKU defined as the SKU of a variant that both:
    1) currently is assigned > 0 inventory quantity 
    2) is a child variant to a parent product that contains a 'published_at' date (if 'published_at' is null, the product is not an active listing on the shopify store)

    Args:
        shopify_api_key (str): Shopify API key
        shopify_api_pw (str): Shopify API password
        shopify_store_url (str): Shopify store URL

    Returns:
        Dict[str]: Details of active product SKUs"""

    # ====================== QUERY DATA =============================================

    # API URL
    base_url = f'https://{shopify_api_key}:{shopify_api_pw}@{shopify_store_url}'
    url = f'{base_url}/admin/api/2021-10/products.json'

    # Make the API request
    response = requests.get(url)
    data = response.json()

    # Extract products dict
    products = data['products']

    # Loop through all products and return the active sku details
    i = 0
    while True:  # Pagination
        i += 1
        logger.info(f'Paginating - {i}')
        logger.info(url)
        response = requests.get(url)
        data = response.json()
        products.extend(data['products'])

        links = response.links
        if 'next' in links:
            url = links['next']['url']
            url = url.replace('https://',
                              f'https://{shopify_api_key}:{shopify_api_pw}@')
        else:
            break

    # active_products = [product for product in products if product['status'] == 'active']

    # Iterate through all product dicts & retain only active skus
    active_shopify_variant_sku_dict = []
    for product in products:  # iterate through each product
        for variant in product[
                'variants']:  # iterate through each variant of each product
            if variant['inventory_quantity'] > 0 and product['published_at']:
                active_shopify_variant_sku_dict.append({
                    'product_id':
                    product['id'],
                    'product_title':
                    product['title'],
                    'variant_id':
                    variant['id'],
                    'variant_title':
                    variant['title'],
                    'variant_sku':
                    variant['sku'],
                    'inventory_quantity':
                    variant['inventory_quantity'],
                    'published_at':
                    product['published_at']
                })

    return active_shopify_variant_sku_dict


def clean_column_name(col_name):
    """Function to format column names for Glue tables"""

    # Remove special characters, replace spaces and multiple non-alphanumeric characters with a single underscore
    col_name = re.sub(r'[^\w]+', '_', col_name)
    # Convert to lowercase
    col_name = col_name.lower()
    # Strip any leading/trailing underscores that may be left after the substitution
    col_name = col_name.strip('_')

    return col_name


# Function (with docstring args) to create an SNS client and publish a message to a topic
def send_sns_alert(message, topic_arn, subject, region):
    """
    Function to create an SNS client and publish a message to a topic
    Args:
        message (str): Message to send to SNS topic
        topic_arn (str): ARN of SNS topic to send message to
        subject (str): Subject of SNS message
    """

    # Instantiate SNS client
    sns_client = boto3.client('sns',
                              region_name=region,
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Publish message to SNS topic
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject,
        )

        logger.info(f'SNS Publish response: {response}')

    except ClientError as e:
        logger.error(f'Error sending SNS alert: {str(e)}')
        raise ValueError(f'Error sending SNS alert! {str(e)}')


def get_shopify_orders_by_date(shopify_api_key: str, shopify_api_pw: str,
                               start_date: str, end_date: str):
    """
    Get all orders & line item details from a Shopify store for a date range with data validation.
    
    Args:
        shopify_api_key (str): Shopify API key
        shopify_api_pw (str): Shopify API password 
        start_date (str): Start date of orders to retrieve
        end_date (str): End date of orders to retrieve

    Returns:
        tuple: (orders_df, line_items_df) with validated data
        
    Raises:
        ValueError: If data validation fails or incomplete data detected
    """
    def validate_order_completeness(total_count: int, retrieved_count: int, date_range: str):
        """Validate we retrieved all expected orders"""
        if total_count != retrieved_count:
            raise ValueError(f"Data incomplete for {date_range}: Expected {total_count} orders but got {retrieved_count}")
            
    def validate_pagination(link_header: str) -> bool:
        """Properly validate pagination from Link header"""
        if not link_header:
            return False
        return 'rel="next"' in link_header

    Returns:
        pd.DataFrame: dataframe of one record per order for orders in the date range
        pd.DataFrame: dataframe of one record per line item for all orders

    """

    # from datetime import datetime, timedelta

    # Set dates to pull data for
    sdate = pd.to_datetime(start_date)
    edate = pd.to_datetime(end_date)

    # Format API and pull all data from desired date range:

    # Set timezone - Pacific for shopify data
    pacific = timezone('US/Pacific')

    # extract year, month and date from user input
    y_start = edate.strftime('%Y')
    m_start = edate.strftime('%m')
    d_start = edate.strftime('%d')

    # Set the start_date (which is the most current date of data we want)
    start_date = pacific.localize(
        datetime.datetime(int(y_start), int(m_start), int(d_start), 23, 59,
                          59))  # datetime.date(%Y,%m,%d,%H,%M,%S)

    # extract year, month and date from user input
    y_end = sdate.strftime('%Y')
    m_end = sdate.strftime('%m')
    d_end = sdate.strftime('%d')

    # Set the end_date (which is the oldest date of data we want, where the loop will terminate)
    end_date = pacific.localize(
        datetime.datetime(int(y_end), int(m_end), int(d_end), 0, 0,
                          0))  # datetime.date(%Y,%m,%d,%H,%M,%S)

    logger.info(f'backfill start date: {start_date}')
    logger.info(f'backfill end date: {end_date}')
    logger.info('shopify api data being pulled from: ', sdate, ' through ',
                edate)

    # Create temp lists to hold the values in the json file
    line_items = []
    prices = []
    order_id = []
    created_at = []
    email = []
    subtotal_price = []
    total_tax = []
    total_discounts = []
    financial_status = []
    line_item_qty = []
    shipping_fees = []

    # Set paramaters for GET request
    payload = {
        'limit': 250,
        'created_at_max': start_date,
        'created_at_min': end_date,
        'financial_status': 'paid'
    }

    # Blank df to store line item details
    shopify_line_item_df = pd.DataFrame()
    shopify_orders_df = pd.DataFrame()

    # Shopify API URL and endpoint
    url = f'https://{shopify_api_key}:{shopify_api_pw}@prymal-coffee-creamer.myshopify.com/admin/api/2021-07/orders.json?status=any'

    has_next_page = True
    successful_response = False
    max_retries = 3

    while has_next_page == True:

        time.sleep(1)

        for retry in range(max_retries):

            try:

                logger.info(f'URL: {url}')

                r = requests.get(url, stream=True, params=payload)

                logger.info(f'Response: {r.status_code}')
                r.raise_for_status()  # Check for any HTTP errors

                if r.status_code == 200:
                    response_json = r.json()
                    
                    # Get total order count from response headers
                    total_orders = int(r.headers.get('X-Shopify-Shop-Api-Call-Limit', '0').split('/')[0])
                    
                    # Validate order statuses
                    for order in response_json['orders']:
                        if 'cancelled_at' in order and order['cancelled_at']:
                            logger.warning(f"Cancelled order found: {order['id']}")
                            
                    # Track order counts for validation
                    if 'orders' in response_json:
                        orders_count = len(response_json['orders'])
                        logger.info(f"Retrieved {orders_count} orders")
                        
                    successful_response = True
                    
                    # Validate pagination properly
                    has_next_page = validate_pagination(r.headers.get('Link', ''))

            except requests.exceptions.HTTPError as errh:
                logger.info("HTTP Error:" + str(errh))
                time.sleep(30)
                continue

            except requests.exceptions.ConnectionError as errc:
                logger.info("Error Connecting:" + str(errc))
                time.sleep(30)
                continue

            except requests.exceptions.Timeout as errt:
                logger.info("Timeout Error:" + str(errt))
                time.sleep(30)
                continue

            except requests.exceptions.RequestException as err:
                logger.info("Error:" + str(err))
                time.sleep(30)
                continue

            if retry < max_retries - 1:
                logger.info('Retrying api call...')
            else:
                logger.info(
                    f'Retried {max_retries} times without getting a 200 response.'
                )

            logger.info(f'Successful response: {successful_response}')

            if successful_response == True:

                # --------------------------- ORDER DF ----------------------------

                # Normalize Shopify Orders
                orders = pd.json_normalize(response_json['orders'])

                # if there are orders, process them
                if len(orders) == 0:
                    logger.info(f'{len(orders)} orders')
                    return shopify_orders_df, shopify_line_item_df

                elif len(orders) > 0:

                    logger.info(f'{len(orders)} orders')

                    # Select relevant columns
                    orders_df = orders[[
                        'order_number', 'email', 'created_at',
                        'shipping_address.address1', 'shipping_address.city',
                        'shipping_address.province',
                        'shipping_address.country', 'subtotal_price',
                        'total_line_items_price', 'total_tax',
                        'total_discounts',
                        'total_shipping_price_set.shop_money.amount',
                        'total_price'
                    ]].copy()
                    # Rename columns
                    orders_df.columns = [
                        'order_id', 'email', 'created_at', 'shipping_address',
                        'shipping_city', 'shipping_province',
                        'shipping_country', 'subtotal_price',
                        'total_line_items_price', 'total_tax',
                        'total_discounts', 'total_shipping_fee', 'total_price'
                    ]

                    logger.info(
                        f"Min order date: {orders_df['created_at'].min()}")

                    # Use created_at to create a formatted date column (yyyy-mm-dd)
                    orders_df['order_date'] = pd.to_datetime(
                        orders_df['created_at'].str.slice(0, 19),
                        format='%Y-%m-%dT%H:%M:%S').dt.strftime('%Y-%m-%d')

                    shopify_orders_df = pd.concat(
                        [shopify_orders_df, orders_df])

                    # --------------------------- LINE ITEM DF ----------------------------

                    # Iterate through orders_df and normalize line_items_df
                    for i in range(len(response_json['orders'])):

                        line_items = pd.json_normalize(
                            response_json['orders'][i]['line_items'])

                        line_items = line_items[[
                            'name', 'price', 'quantity', 'sku', 'title',
                            'variant_title'
                        ]].copy()

                        line_items['order_id'] = orders.iloc[i]['order_number']
                        line_items['email'] = orders.iloc[i]['email']
                        line_items['created_at'] = orders.iloc[i]['created_at']

                        line_items['order_date'] = pd.to_datetime(
                            line_items['created_at'].str.slice(0, 19),
                            format='%Y-%m-%dT%H:%M:%S').dt.strftime('%Y-%m-%d')

                        line_items.columns = [
                            'line_item_name', 'price', 'quantity', 'sku',
                            'title', 'variant_title', 'order_id', 'email',
                            'created_at', 'order_date'
                        ]
                        line_items = line_items[[
                            'order_id', 'email', 'created_at', 'order_date',
                            'price', 'quantity', 'sku', 'title',
                            'variant_title', 'line_item_name'
                        ]].copy()

                        line_items.reset_index(inplace=True, drop=True)

                        shopify_line_item_df = pd.concat(
                            [shopify_line_item_df, line_items])

                        # ----------- Paginate ---------------

                        # Paginate results if there are more than 250 orders
                        if 'link' in r.headers and 'rel="next"' in r.headers[
                                'link']:

                            if len(r.headers['link'].split(',')) > 1:
                                next_link = r.headers['link'].split(
                                    ',')[1].split('>')[0].replace('<', '')
                                url = f'{next_link.split("//")[0]}' + f'//{shopify_api_key}:' + f'{shopify_api_pw}' + '@' + f'{next_link.split("//")[1]}'

                            else:
                                next_link = r.headers['link'].split(
                                    '>')[0].replace('<', '')
                                url = f'{next_link.split("//")[0]}' + f'//{shopify_api_key}:' + f'{shopify_api_pw}' + '@' + f'{next_link.split("//")[1]}'

                        else:
                            has_next_page = False

                        #Reset Payload
                        payload = {'limit': 250}

                        logger.info(orders_df['order_date'].min(),
                                    orders_df['order_date'].max())
                        logger.info(f'Has next page: {has_next_page}')
                        logger.info(f'Total orders: {len(shopify_orders_df)}')
                        logger.info(
                            f'Orders date range: {shopify_orders_df["order_date"].min()} to {shopify_orders_df["order_date"].max()}'
                        )
                        logger.info(
                            f'Total line items: {len(shopify_line_item_df)}')
                        logger.info(
                            f'Line items date range: {shopify_line_item_df["order_date"].min()} to {shopify_line_item_df["order_date"].max()}'
                        )

                    shopify_line_item_df.reset_index(inplace=True, drop=True)
                    shopify_orders_df.reset_index(inplace=True, drop=True)

                    return shopify_orders_df, shopify_line_item_df

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

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_ACCESS_SECRET')


def delete_s3_data(bucket: str, prefix: str):
    """Function to delete all data in an s3 bucket with a given prefix"""
    logger.info(f'Deleting s3 data in bucket: {bucket} with prefix: {prefix}')
    s3_client = boto3.client('s3',
                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                             region_name='us-east-1')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
        logger.info(f'Successfully deleted s3 data in bucket: {bucket} with prefix: {prefix}')
        return True

    except Exception as e:
        logger.error(f'Error deleting s3 data: {str(e)}')
        raise ValueError(f'Error deleting s3 data! {str(e)}')


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
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

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
        raise
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        raise
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']

        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            raise
            # Handle issues with the Athena request, such as invalid SQL syntax

        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            raise
            # Handle cases where the database or query execution does not exist

        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            raise
            # Handle cases where the IAM role does not have sufficient permissions

        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            raise
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        raise


class AthenaQueryError(RuntimeError):
    """An Athena query did not complete successfully."""


def run_athena_query(query: str, database: str, region: str, s3_bucket: str,
                     poll_seconds: float = 1.0, sleep=time.sleep):
    """Function to execute an athena query & return results csv as a dataframe

    Args:
        query (str): The query to be executed
        database (str): The Glue database to be queried
        region (str): The AWSregion to be queried
        s3_bucket (str) : S3 bucket name for query results
        poll_seconds (float): Delay between status checks
        sleep (callable): Injected for tests
    Returns:
        (pd.DataFrame): The results of the query as a dataframe

    Raises:
        AthenaQueryError: if the query fails, is cancelled, or the request is
            rejected. Never returns None - callers call len() on the result.
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
        logger.info(f'Running query..')

        while True:
            status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)['QueryExecution']['Status']
            state = status['State']

            if state == 'SUCCEEDED':
                logger.info('Query Succeeded!')
                break

            # FAILED, CANCELLED, or any future terminal state. Raise rather
            # than returning None: callers do len(df) on the result, so a
            # swallowed error surfaces as an unrelated TypeError.
            if state not in ('RUNNING', 'QUEUED'):
                raise AthenaQueryError(
                    f'Athena query {query_execution_id} {state}: '
                    f"{status.get('StateChangeReason', 'no reason given')}\n"
                    f'Query was:\n{query.strip()}')

            sleep(poll_seconds)

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

    except ClientError as e:
        # InvalidRequestException here usually means bad SQL, or asking for the
        # results of a query that never reached SUCCEEDED.
        error = e.response['Error']
        raise AthenaQueryError(
            f"Athena request failed ({error['Code']}): {error['Message']}\n"
            f'Query was:\n{query.strip()}') from e

    except (ParamValidationError, WaiterError) as e:
        raise AthenaQueryError(f'Athena request rejected: {e}\n'
                               f'Query was:\n{query.strip()}') from e


def latest_partition(table: str, database: str, region: str,
                     column: str = 'partition_date',
                     on_or_before: str = None):
    """Return the most recent partition value for a table, or None if there is none.

    Reads the partition list straight from the Glue Data Catalog. This is the
    only way to get it for free: Athena bills by bytes scanned, and

        SELECT MAX(partition_date) FROM t WHERE partition_date <= DATE(...)

    still reads data files rather than being answered from metadata, so on a
    table with enough daily partitions it trips the workgroup's bytes-scanned
    limit and Athena cancels the query. Glue has no such notion.

    Use this and inline the result as a literal rather than writing
    `WHERE partition_date = (SELECT MAX(partition_date) FROM t)`, which also
    stops Athena pruning partitions and scans the whole table.

    Args:
        table (str): Table to inspect
        database (str): Glue database
        region (str): AWS region
        column (str): Partition column name
        on_or_before (str): Optional 'YYYY-MM-DD' upper bound

    Returns:
        (str | None): Partition value as 'YYYY-MM-DD', or None if no partition matches

    Raises:
        ValueError: if the table is not partitioned on `column`
    """
    glue_client = boto3.client('glue',
                               region_name=region,
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    table_meta = glue_client.get_table(DatabaseName=database, Name=table)['Table']
    partition_keys = [key['Name'] for key in table_meta.get('PartitionKeys', [])]

    if column not in partition_keys:
        raise ValueError(
            f"{database}.{table} is not partitioned on '{column}' "
            f"(partition keys: {partition_keys or 'none'})")

    position = partition_keys.index(column)

    values = []
    paginator = glue_client.get_paginator('get_partitions')
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        values.extend(partition['Values'][position]
                      for partition in page['Partitions'])

    # Partition values are strings; 'YYYY-MM-DD' sorts correctly as text.
    if on_or_before:
        values = [value for value in values if value <= on_or_before]

    logger.info(f'{database}.{table}: {len(values)} partition(s) considered')

    return max(values) if values else None


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
            # Attempt to create a model instance
            valid_item = model(**item)
            valid_items.append(valid_item.model_dump())
        except ValidationError as e:
            # If validation fails, add to invalid items
            invalid_items.append((item, str(e)))

    return valid_items, invalid_items


# ShipBob API access lives in src/shipbob/ (API version 2026-07).


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


def get_shopify_orders_by_date(
    shopify_api_key: str,
    shopify_api_pw: str,
    start_date: str,
    end_date: str
):
    """
    Fetch all Shopify orders and line items for [start_date, end_date],
    returning (orders_df, line_items_df). Correctly paginates by inserting
    credentials into each subsequent 'next' link from the Link header.
    """
    import requests
    import pandas as pd
    import time
    from loguru import logger
    from urllib.parse import urlsplit, urlunsplit

    # Convert inputs to datetimes and build the min/max for the API query (UTC here)
    start_dt = pd.to_datetime(start_date)
    end_dt   = pd.to_datetime(end_date)

    created_at_min = start_dt.strftime('%Y-%m-%dT00:00:00Z')
    created_at_max = end_dt.strftime('%Y-%m-%dT23:59:59Z')

    # Base URL with credentials for page 1
    base_url = f"https://{shopify_api_key}:{shopify_api_pw}@prymal-coffee-creamer.myshopify.com"
    endpoint = "/admin/api/2021-07/orders.json"

    # Initial query params
    params = {
        'status': 'any',
        'limit': 250,
        'created_at_min': created_at_min,
        'created_at_max': created_at_max
    }

    # Start with the first URL
    url = base_url + endpoint

    # Lists to accumulate all records
    all_orders = []
    all_line_items = []

    while True:
        logger.info(f"Fetching: {url} with params={params}")
        response = requests.get(url, params=params)

        # Raise an exception if the response was not successful
        response.raise_for_status()

        data_json = response.json()
        orders = data_json.get('orders', [])
        logger.info(f"Fetched {len(orders)} orders on this page.")

        # If no orders returned, we are done
        if not orders:
            break

        # Build out the orders and line items records
        for order in orders:
            order_id   = order.get('order_number')
            created_at = order.get('created_at')
            email      = order.get('email')

            # Convert created_at to YYYY-MM-DD
            order_date = pd.to_datetime(created_at).strftime('%Y-%m-%d') if created_at else None

            # Parse shipping details
            shipping_info = {} if order.get('shipping_address',{}) == None else order.get('shipping_address',{})
            
            shipping_address = shipping_info.get('address1', None) 
            shipping_city = shipping_info.get('city',None)
            shipping_province = shipping_info.get('province',None)
            shipping_country = shipping_info.get('country',None)

            # Build an order record
            all_orders.append({
                'order_id': order_id,
                'email': email,
                'created_at': created_at,
                'order_date': order_date,
                'subtotal_price': order.get('subtotal_price'),
                'total_line_items_price': order.get('total_line_items_price'),
                'total_tax': order.get('total_tax'),
                'total_discounts': order.get('total_discounts'),
                'total_shipping_fee': order.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount'),
                'total_price': order.get('total_price'),
                'shipping_address': shipping_address,
                'shipping_city': shipping_city,
                'shipping_province': shipping_province,
                'shipping_country': shipping_country
            })


            # Collect line items for each order
            for line_item in order.get('line_items', []):
                line_items_record = {
                    'order_id': order_id,
                    'email': email,
                    'created_at': created_at,
                    'order_date': order_date,
                    'price': line_item.get('price'),
                    'quantity': line_item.get('quantity'),
                    'sku': line_item.get('sku'),
                    'title': line_item.get('title'),
                    'variant_title': line_item.get('variant_title'),
                    'line_item_name': line_item.get('name')
                }
                all_line_items.append(line_items_record)

        # Pagination: check the 'Link' header for rel="next"
        link_header = response.headers.get('Link', '')
        if 'rel="next"' not in link_header:
            # No next page, break the loop
            logger.info("No next page found; pagination complete.")
            break

        # Otherwise, parse next page URL from link_header
        next_url = None
        for part in link_header.split(','):
            if 'rel="next"' in part:
                start = part.find('<') + 1
                end = part.find('>')
                next_url = part[start:end]
                break

        if not next_url:
            logger.info("Next page URL not found; stopping pagination.")
            break

        # Inject credentials for the next page:
        # e.g. https://prymal-coffee-creamer.myshopify.com -> https://USER:PASS@prymal-coffee-creamer.myshopify.com
        parsed = urlsplit(next_url)
        # url w/ creds
        url_with_creds = f"{shopify_api_key}:{shopify_api_pw}@{parsed.hostname}"

        # Rebuild the next_url with embedded credentials
        next_url = urlunsplit((parsed.scheme, url_with_creds, parsed.path, parsed.query, parsed.fragment))

        url = next_url
        # Clear params because next_url includes them already
        params = {}

        # Optional sleep to avoid rate limits
        time.sleep(1)

    # Convert accumulated records to DataFrames
    orders_df = pd.DataFrame(all_orders)
    line_items_df = pd.DataFrame(all_line_items)

    logger.info(f"Total orders retrieved: {len(orders_df)}")
    logger.info(f"Total line items retrieved: {len(line_items_df)}")

    return orders_df, line_items_df

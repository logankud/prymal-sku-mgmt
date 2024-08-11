import boto3
import botocore
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
from loguru import logger
import os
from io import StringIO
import requests
import json
import pandas as pd
import numpy as np
import csv
import datetime
from datetime import timedelta
from typing import Any, List, Tuple, Type, Dict, get_origin, get_args

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
            df[col] = df[col].apply(lambda x: x.replace('"', "'") if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: x.replace(',', ';') if isinstance(x, str) else x)
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
        raise ValueError(f'Error formatting dataframe for writing to s3! {str(e)}')
        
    logger.info(df.dtypes)
    logger.info(f'Writing df to csv {key}')

    # Use s3 client to write dataframe to S3 as csv
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,
                  index=False,
                  encoding='utf-8')
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

    else:

        rec = response_json

        # Extract inventory details for the product
        inventory_items = pd.json_normalize(rec['fulfillable_inventory_items'])

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

    while 'Next-Page' in response.headers and int(current_page) <= int(
            total_pages):

        url_params = response.headers['Next-Page']

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

        else:

            rec = response_json

            # Extract inventory details for the product
            inventory_items = pd.json_normalize(
                rec['fulfillable_inventory_items'])

            # Rename columns
            inventory_items.columns = [
                'inventory_id', 'inventory_name', 'inventory_qty'
            ]

            # Carry forward relevant fields
            inventory_items['sku'] = rec['sku']
            inventory_items['sku_name'] = rec['name']

            # Append results to df
            product_to_inventory_df = pd.concat(
                [product_to_inventory_df, inventory_items])

            logger.info(f'Page {current_page} of {total_pages} retrieved')

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

    url = 'https://api.shipbob.com'
    url_params = f'/1.0/order?StartDate={start_date}&EndDate={end_date}'

    # Set up the request headers with the Bearer token
    headers = {"Authorization": f"Bearer {api_secret}"}

    # Blank df to store results
    shipbob_order_details_df = pd.DataFrame(columns=[
        'created_date', 'purchase_date', 'shipbob_order_id', 'order_number',
        'order_status', 'order_type', 'channel_id', 'channel_name',
        'product_id', 'sku', 'shipping_method', 'customer_name',
        'customer_email', 'customer_address_city', 'customer_address_state',
        'customer_address_country'
    ])

    # Send the GET request
    response = requests.get(url + url_params, headers=headers)

    # convert to json
    response_json = json.loads(response.text)

    # Parse response json to a df (one record per line item in an order)
    if type(response_json) == list:

        for rec in response_json:

            # Extract product / line item details for the order
            line_items = pd.json_normalize(rec['products'])

            if len(line_items) > 0:

                # Subset columns to retain
                line_items = line_items[['id', 'sku']].copy()

                # Rename columns
                line_items.columns = ['product_id', 'sku']

                # Carry forward relevant fields
                line_items['shipbob_order_id'] = rec['id']
                line_items['created_date'] = pd.to_datetime(
                    rec['created_date'])
                line_items['purchase_date'] = pd.to_datetime(
                    rec['purchase_date'])
                line_items['order_number'] = rec['order_number']
                line_items['order_status'] = rec['status']
                line_items['order_type'] = rec['type']
                line_items['shipping_method'] = rec['shipping_method']

                # Channel details
                line_items['channel_id'] = rec['channel']['id']
                line_items['channel_name'] = rec['channel']['name']

                # Recipient details
                line_items['customer_name'] = rec['recipient']['name']
                line_items['customer_email'] = rec['recipient']['email']
                line_items['customer_address_city'] = rec['recipient'][
                    'address']['city']
                line_items['customer_address_state'] = rec['recipient'][
                    'address']['state']
                line_items['customer_address_country'] = rec['recipient'][
                    'address']['country']

                # If records exist, append them
                if len(line_items) > 0:
                    # Append results to df
                    shipbob_order_details_df = pd.concat(
                        [shipbob_order_details_df, line_items])

    else:

        rec = response_json

        # Extract product / line item details for the order
        line_items = pd.json_normalize(rec['products'])

        if len(line_items) > 0:

            # Subset columns to retain
            line_items = line_items[['id', 'sku']].copy()

            # Rename columns
            line_items.columns = ['product_id', 'sku']

            # Carry forward relevant fields
            line_items['shipbob_order_id'] = rec['id']
            line_items['created_date'] = pd.to_datetime(rec['created_date'])
            line_items['purchase_date'] = pd.to_datetime(rec['purchase_date'])
            line_items['order_number'] = rec['order_number']
            line_items['order_status'] = rec['status']
            line_items['order_type'] = rec['type']
            line_items['shipping_method'] = rec['shipping_method']

            # Channel details
            line_items['channel_id'] = rec['channel']['id']
            line_items['channel_name'] = rec['channel']['name']

            # Recipient details
            line_items['customer_name'] = rec['recipient']['name']
            line_items['customer_email'] = rec['recipient']['email']
            line_items['customer_address_city'] = rec['recipient']['address'][
                'city']
            line_items['customer_address_state'] = rec['recipient']['address'][
                'state']
            line_items['customer_address_country'] = rec['recipient'][
                'address']['country']

            # Append results to df
            shipbob_order_details_df = pd.concat(
                [shipbob_order_details_df, line_items])

    # Extract page details
    current_page = response.headers['Page-Number']
    total_pages = response.headers['Total-Pages']

    while 'Next-Page' in response.headers and int(current_page) <= int(
            total_pages):

        url_params = response.headers['Next-Page']

        # Send the GET request
        response = requests.get(url + url_params, headers=headers)

        # convert to json
        response_json = json.loads(response.text)

        # Parse response json to a df (one record per line item in an order)
        if type(response_json) == list:

            for rec in response_json:

                # Extract product / line item details for the order
                line_items = pd.json_normalize(rec['products'])

                if len(line_items) > 0:

                    # Subset columns to retain
                    line_items = line_items[['id', 'sku']].copy()

                    # Rename columns
                    line_items.columns = ['product_id', 'sku']

                    # Carry forward relevant fields
                    line_items['shipbob_order_id'] = rec['id']
                    line_items['created_date'] = pd.to_datetime(
                        rec['created_date'])
                    line_items['purchase_date'] = pd.to_datetime(
                        rec['purchase_date'])
                    line_items['order_number'] = rec['order_number']
                    line_items['order_status'] = rec['status']
                    line_items['order_type'] = rec['type']
                    line_items['shipping_method'] = rec['shipping_method']

                    # Channel details
                    line_items['channel_id'] = rec['channel']['id']
                    line_items['channel_name'] = rec['channel']['name']

                    # Recipient details
                    line_items['customer_name'] = rec['recipient']['name']
                    line_items['customer_email'] = rec['recipient']['email']
                    line_items['customer_address_city'] = rec['recipient'][
                        'address']['city']
                    line_items['customer_address_state'] = rec['recipient'][
                        'address']['state']
                    line_items['customer_address_country'] = rec['recipient'][
                        'address']['country']

                    # Append results to df
                    shipbob_order_details_df = pd.concat(
                        [shipbob_order_details_df, line_items])

        else:

            rec = response_json

            # Extract product / line item details for the order
            line_items = pd.json_normalize(rec['products'])

            if len(line_items) > 0:

                # Subset columns to retain
                line_items = line_items[['id', 'sku']].copy()

                # Rename columns
                line_items.columns = ['product_id', 'sku']

                # Carry forward relevant fields
                line_items['shipbob_order_id'] = rec['id']
                line_items['created_date'] = pd.to_datetime(
                    rec['created_date'])
                line_items['purchase_date'] = pd.to_datetime(
                    rec['purchase_date'])
                line_items['order_number'] = rec['order_number']
                line_items['order_status'] = rec['status']
                line_items['order_type'] = rec['type']
                line_items['shipping_method'] = rec['shipping_method']

                # Channel details
                line_items['channel_id'] = rec['channel']['id']
                line_items['channel_name'] = rec['channel']['name']

                # Recipient details
                line_items['customer_name'] = rec['recipient']['name']
                line_items['customer_email'] = rec['recipient']['email']
                line_items['customer_address_city'] = rec['recipient'][
                    'address']['city']
                line_items['customer_address_state'] = rec['recipient'][
                    'address']['state']
                line_items['customer_address_country'] = rec['recipient'][
                    'address']['country']

                # Append results to df
                shipbob_order_details_df = pd.concat(
                    [shipbob_order_details_df, line_items])

        # Extract page details
        current_page = response.headers['Page-Number']

        logger.info(f'Page {current_page} of {total_pages} retrieved')
        logger.info(
            f"Min Date: {shipbob_order_details_df['created_date'].min()}  / Max Date: {shipbob_order_details_df['created_date'].max()}"
        )
        logger.info(
            f"Total distinct records: {shipbob_order_details_df['order_number'].nunique()}"
        )

    return shipbob_order_details_df.reset_index(drop=True)


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

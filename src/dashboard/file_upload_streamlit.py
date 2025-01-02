import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import *
import os
import re
from loguru import logger
import bcrypt

import sys

sys.path.append('src/')  # updating path back to root for importing modules
from utils import validate_dataframe, write_df_to_s3
from models import KatanaInventory, KatanaRecipeIngredient, ManufacturingOrder

# AWS S3 Configuration
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_REGION = 'us-east-1'

# Streamlit server configuration
st.set_page_config(page_title="CSV Uploader",
                   layout="wide",
                   initial_sidebar_state="expanded")

# Session variable
CURRENT_DATE = pd.to_datetime('today').strftime('%Y-%m-%d')
CURRENT_DATE_Y = pd.to_datetime('today').strftime('%Y')
CURRENT_DATE_M = pd.to_datetime('today').strftime('%m')
CURRENT_DATE_D = pd.to_datetime('today').strftime('%d')

if __name__ == '__main__':



    # User credentials
    USERS = {
        f"{os.getenv('AUTH_USER_TRAVIS')}": bcrypt.hashpw(f"{os.getenv('AUTH_PW_TRAVIS')}".encode(), bcrypt.gensalt()),
    }

    def authenticate(username, password):
        if username in USERS and bcrypt.checkpw(password.encode(), USERS[username]):
            return True
        return False

    def login():
        st.sidebar.title("Login")
        username = st.sidebar.text_input("Username")
        password = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Login"):
            if authenticate(username, password):
                st.session_state["authenticated"] = True
                st.sidebar.success("Authentication successful!")
            else:
                st.sidebar.error("Invalid username or password!")

    # Main app logic
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        login()
    else:
        st.sidebar.success("Welcome, logged in!")

        logger.info("Launching app..")
    
    
        st.title("Prymal - Doc Upload")
    
        # Add a radio button to select the file type
        file_type = st.radio(
            "Select the file type to upload:",
            ("Katana Inventory", "Katana MO", "Katana Recipes"),
        )
    
        uploaded_file = st.file_uploader(f"Choose a CSV file for {file_type}",
                                         type=["csv"])
    
        if uploaded_file is not None:
    
            # Read CSV into DataFrame
            df = pd.read_csv(uploaded_file)
    
            # Display DataFrame
            st.dataframe(df)
    
            # Display a button to trigger the upload process
            if st.button("Upload"):
    
                try:
                    st.write("File upload in progress..")
    
                    logger.info(f'Initial columns: {df.columns}')
    
                    # format columns
                    cols_clean = [
                        'mo' if col == 'MO #' else re.sub(
                            r'[^a-z0-9_]', '', re.sub(r'\s+|/|#', '_',
                                                      col.lower()))
                        for col in df.columns
                    ]
                    df.columns = cols_clean
    
                    logger.info(f'Columns (cleaned): {cols_clean}')
                    
                    cols_clean = [
                        col.replace('___', '_') if '___' in col else col
                        for col in cols_clean
                    ]  # handle triple underscores
    
                    logger.info(f'Columns (cleaned): {cols_clean}')
    
    
                    # add'l formatting
                    cols_clean = [
                        col.replace('__', '_') if '__' in col else col
                        for col in cols_clean  
                    ]  # handle double underscores
    
                    
    
                    logger.info(f'Columns (cleaned): {cols_clean}')
    
                    # add'l formatting
                    cols_clean = [
                        col[:-1] if col.endswith('_') else col
                        for col in cols_clean
                    ]  # strip trailing underscore
    
                    logger.info(f'Columns (cleaned): {cols_clean}')
    
                    # Replace column names
                    df.columns = cols_clean
    
                    st.dataframe(df)
    
                    # Select the appropriate schema model and S3 path
                    if file_type == "Katana Inventory":
                        schema_model = KatanaInventory
                        s3_prefix = f"katana/inventory/partition_date={CURRENT_DATE}/katana_inventory_{CURRENT_DATE.replace('-','_')}.csv"
                        glue_table = "katana_inventory"
    
                    elif file_type == "Katana MO":
                        schema_model = ManufacturingOrder
                        s3_prefix = f"katana/open_manufacturing_orders/partition_date={CURRENT_DATE}/katana_open_manufacturing_orders_{CURRENT_DATE.replace('-','_')}.csv"
                        glue_table = "katana_open_manufacturing_orders"
    
                    elif file_type == "Katana Recipes":
                        schema_model = KatanaRecipeIngredient
                        s3_prefix = f"katana/formulas/partition_date={CURRENT_DATE}/katana_formulas_{CURRENT_DATE.replace('-','_')}.csv"
                        glue_table = "katana_formulas"
    
                    else:
                        st.error("Invalid file type selected.")
                        st.stop()
    
                    # Validate data w/ Pydantic
                    valid_data, invalid_data = validate_dataframe(df, schema_model)
    
                    if len(invalid_data) > 0:
                        for invalid in invalid_data:
                            st.error("Invalid Data!")
                            st.error([err for err in invalid_data])
                            raise ValueError(f'Invalid data!')
    
                    if len(valid_data) > 0:
                        # Instantiate s3 client
                        s3_client = boto3.client(
                            's3',
                            region_name=S3_REGION,
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                            aws_secret_access_key=os.getenv('AWS_ACCESS_SECRET'))
    
                        try:
                            # Write to s3
                            try:
                                write_df_to_s3(bucket=S3_BUCKET,
                                               key=s3_prefix,
                                               df=pd.DataFrame(valid_data),
                                               s3_client=s3_client)
                                st.success("File successfully uploaded!")
                            except Exception as e:
                                st.error("File failed to upload!")
    
                        except Exception as e:
                            st.error(f'Error writing to s3: {str(e)}')
                            raise ValueError(f'Error writing data! {str(e)}')
    
                except Exception as e:
                    st.error(f"An error occurred: {e}")
    
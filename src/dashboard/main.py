import dash
from dash import dcc, html
import pandas as pd
import sys
import os
import numpy as np
from loguru import logger

sys.path.append('src/')  # If you need to import from your src/ folder
from utils import run_athena_query

# Import your two tab/page modules
from product_cards import get_product_cards_tab, register_product_cards_callbacks
from product_summary import get_product_summary_tab, register_product_summary_callbacks

# AWS Athena configuration
REGION = 'us-east-1'  # e.g., 'us-east-1'
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
GLUE_DATABASE = os.getenv('GLUE_DATABASE_NAME')

# Initialize the Dash app
app = dash.Dash(__name__)
app.title = "Prymal Inventory Dashboard"

###############################################################################
#                           DATA LOADING / CACHING
###############################################################################
inventory_run_rate_df_cached = None
merged_df_cached = None
product_options = None
inventory_details_df_cached = None
est_stock_days_on_hand_min = None
est_stock_days_on_hand_max = None


def load_data():
    """
    Load all data from Athena (or other sources),
    then return it. This is the same logic from your original dashboard.py.
    """

    # 1) Fetch all inventory data for the latest partition_date
    inventory_query = """
    SELECT *
    FROM shipbob_inventory_run_rate
    WHERE partition_date = (SELECT MAX(partition_date) FROM shipbob_inventory_run_rate)
    """
    inventory_run_rate_df = run_athena_query(query=inventory_query,
                                             database=GLUE_DATABASE,
                                             region=REGION,
                                             s3_bucket=S3_BUCKET)

    # 2) Fetch order details data for the past 90 days
    order_details_query = """
    SELECT 
        DATE(created_date) as created_date,
        inventory_name,
        inventory_id,
        SUM(inventory_qty) as inventory_qty
    FROM shipbob_order_details 
    WHERE created_date >= date_add('day', -90, current_date)
    GROUP BY DATE(created_date),
             inventory_name,
             inventory_id
    ORDER BY DATE(created_date) ASC
    """
    order_details_df = run_athena_query(query=order_details_query,
                                        database=GLUE_DATABASE,
                                        region=REGION,
                                        s3_bucket=S3_BUCKET)

    # Convert data types
    order_details_df['created_date'] = pd.to_datetime(
        order_details_df['created_date'])
    order_details_df['inventory_qty'] = pd.to_numeric(
        order_details_df['inventory_qty'])
    order_details_df['inventory_id'] = pd.to_numeric(
        order_details_df['inventory_id'])

    # 3) Calculate quantities sold (30, 60, 90 days)
    today = pd.Timestamp.today().normalize()
    date_30_days_ago = today - pd.Timedelta(days=30)
    date_60_days_ago = today - pd.Timedelta(days=60)
    date_90_days_ago = today - pd.Timedelta(days=90)

    sales_last_30_days = order_details_df[order_details_df['created_date'] >= date_30_days_ago]\
                        .groupby('inventory_id')['inventory_qty'].sum().reset_index()\
                        .rename(columns={'inventory_qty': 'actual_qty_sold_last_30_days'})

    sales_last_60_days = order_details_df[order_details_df['created_date'] >= date_60_days_ago]\
                        .groupby('inventory_id')['inventory_qty'].sum().reset_index()\
                        .rename(columns={'inventory_qty': 'actual_qty_sold_last_60_days'})

    sales_last_90_days = order_details_df[order_details_df['created_date'] >= date_90_days_ago]\
                        .groupby('inventory_id')['inventory_qty'].sum().reset_index()\
                        .rename(columns={'inventory_qty': 'actual_qty_sold_last_90_days'})

    # Merge these into inventory_run_rate_df
    inventory_run_rate_df['inventory_id'] = pd.to_numeric(
        inventory_run_rate_df['inventory_id'])
    inventory_run_rate_df = inventory_run_rate_df.merge(sales_last_30_days,
                                                        on='inventory_id',
                                                        how='left')
    inventory_run_rate_df = inventory_run_rate_df.merge(sales_last_60_days,
                                                        on='inventory_id',
                                                        how='left')
    inventory_run_rate_df = inventory_run_rate_df.merge(sales_last_90_days,
                                                        on='inventory_id',
                                                        how='left')
    inventory_run_rate_df[[
        'actual_qty_sold_last_30_days', 'actual_qty_sold_last_60_days',
        'actual_qty_sold_last_90_days'
    ]] = inventory_run_rate_df[[
        'actual_qty_sold_last_30_days', 'actual_qty_sold_last_60_days',
        'actual_qty_sold_last_90_days'
    ]].fillna(0).astype(int)

    # 4) Fetch inventory details data (past 90 days)
    inventory_details_query = """
    SELECT id AS inventory_id, partition_date, total_fulfillable_quantity
    FROM shipbob_inventory_details
    WHERE partition_date >= date_add('day', -90, current_date)
    """
    inventory_details_df = run_athena_query(query=inventory_details_query,
                                            database=GLUE_DATABASE,
                                            region=REGION,
                                            s3_bucket=S3_BUCKET)
    inventory_details_df['partition_date'] = pd.to_datetime(
        inventory_details_df['partition_date'])
    inventory_details_df['total_fulfillable_quantity'] = pd.to_numeric(
        inventory_details_df['total_fulfillable_quantity'])
    inventory_details_df['inventory_id'] = pd.to_numeric(
        inventory_details_df['inventory_id'])

    # Merge inventory_details_df with inventory_run_rate_df to get 'name'
    inventory_details_df = pd.merge(
        inventory_details_df,
        inventory_run_rate_df[['inventory_id', 'name']],
        on='inventory_id',
        how='left')

    # Merge order_details_df with inventory_run_rate_df to get run_rate, stockout_date, etc.
    merged_df = pd.merge(order_details_df,
                         inventory_run_rate_df[[
                             'inventory_id', 'run_rate',
                             'estimated_stockout_date', 'restock_point', 'name'
                         ]],
                         on='inventory_id',
                         how='left')
    merged_df['created_date'] = pd.to_datetime(merged_df['created_date'])
    merged_df['inventory_qty'] = pd.to_numeric(merged_df['inventory_qty'])
    merged_df['run_rate'] = pd.to_numeric(merged_df['run_rate'])
    merged_df['estimated_stockout_date'] = pd.to_datetime(
        merged_df['estimated_stockout_date'])
    merged_df['restock_point'] = pd.to_numeric(merged_df['restock_point'])

    # Convert est_stock_days_on_hand to numeric, drop NaNs
    inventory_run_rate_df['est_stock_days_on_hand'] = pd.to_numeric(
        inventory_run_rate_df['est_stock_days_on_hand'], errors='coerce')
    inventory_run_rate_df = inventory_run_rate_df.dropna(
        subset=['est_stock_days_on_hand'])
    inventory_run_rate_df['est_stock_days_on_hand'] = \
        inventory_run_rate_df['est_stock_days_on_hand'].astype(float).round().astype(int)

    # Calculate slider min and max
    est_stock_days_on_hand_min = inventory_run_rate_df[
        'est_stock_days_on_hand'].min()
    est_stock_days_on_hand_max = inventory_run_rate_df[
        'est_stock_days_on_hand'].max()

    # Build product_options list
    product_options = [{
        'label': name,
        'value': name
    } for name in sorted(inventory_run_rate_df['name'].unique())]

    return (inventory_run_rate_df, merged_df, product_options,
            inventory_details_df, est_stock_days_on_hand_min,
            est_stock_days_on_hand_max)


# Load data once at startup
(inventory_run_rate_df_cached, merged_df_cached, product_options,
 inventory_details_df_cached, est_stock_days_on_hand_min,
 est_stock_days_on_hand_max) = load_data()

logger.info(f'Product options: {product_options}')

###############################################################################
#                             APP LAYOUT
###############################################################################

# We build the entire app layout here, with the two tabs.
# We'll import each tab's content from product_cards.py and product_summary.py
app.layout = html.Div([
    dcc.Store(id='hidden-cards-store', data=[]),
    dcc.Store(id='selected-product-store', data=None),
    dcc.Tabs(
        id='tabs',
        value='product-cards',
        children=[
            # Product Cards Tab
            get_product_cards_tab(est_stock_days_on_hand_min,
                                  est_stock_days_on_hand_max),
            # Product Summary Tab
            get_product_summary_tab(product_options)
        ])
])

###############################################################################
#                         REGISTER ALL CALLBACKS
###############################################################################

# Register all callbacks for the Product Cards tab
register_product_cards_callbacks(app, inventory_run_rate_df_cached)

# Register all callbacks for the Product Summary tab
register_product_summary_callbacks(app, inventory_run_rate_df_cached,
                                   merged_df_cached,
                                   inventory_details_df_cached)

###############################################################################
#                           RUN THE APP
###############################################################################
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)

# from new_page_x import get_new_page_x_tab, register_new_page_x_callbacks
# ...
# dcc.Tabs(children=[
#     get_product_cards_tab(...),
#     get_product_summary_tab(...),
#     get_new_page_x_tab(...)
# ])
# ...
# register_new_page_x_callbacks(app, ...)

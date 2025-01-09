import dash
from dash import dcc, html, Output, Input, State, ALL, dash_table
import plotly.graph_objs as go
import pandas as pd
import sys
import os
import numpy as np

sys.path.append('src/')  # updating path back to root for importing modules
from utils import run_athena_query  # Ensure this import is correct based on your project structure

# AWS Athena configuration
REGION = 'us-east-1'  # e.g., 'us-east-1'
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
GLUE_DATABASE = os.getenv('GLUE_DATABASE_NAME')

# Initialize the Dash app
app = dash.Dash(__name__)
app.title = "Prymal Inventory Dashboard"

# Create cache dictionary
cache = {
    'data_loaded': False,
    'inventory_run_rate_df': None,
    'merged_df': None,
    'product_options': None,
    'inventory_details_df': None,
    'est_stock_days_on_hand_min': None,
    'est_stock_days_on_hand_max': None
}

def load_data():
    """
    Load and prepare data for the dashboard
    Returns:
        Tuple containing inventory run rate data, merged data, product options,
        inventory details, and stock days on hand min/max values
    """
    # Fetch all inventory data for the latest partition_date
    inventory_query = """
    SELECT *
    FROM shipbob_inventory_run_rate
    WHERE partition_date = (SELECT MAX(partition_date) FROM shipbob_inventory_run_rate)
    """
    inventory_run_rate_df = run_athena_query(query=inventory_query, database=GLUE_DATABASE, region=REGION,
                                           s3_bucket=S3_BUCKET)

    # Fetch order details data for the past 90 days
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
    order_details_df = run_athena_query(query=order_details_query, database=GLUE_DATABASE, region=REGION,
                                      s3_bucket=S3_BUCKET)

    # Process data and return required information
    merged_df = pd.merge(order_details_df, inventory_run_rate_df, on='inventory_id', how='left')
    product_options = [{'label': name, 'value': name} for name in sorted(inventory_run_rate_df['name'].unique())]
    inventory_details_df = pd.DataFrame()  # You may want to modify this based on your needs
    est_stock_days_on_hand_min = inventory_run_rate_df['est_stock_days_on_hand'].min()
    est_stock_days_on_hand_max = inventory_run_rate_df['est_stock_days_on_hand'].max()

    return (inventory_run_rate_df, merged_df, product_options, inventory_details_df,
            est_stock_days_on_hand_min, est_stock_days_on_hand_max)

# Initialize data if not already loaded
if not cache['data_loaded']:
    (cache['inventory_run_rate_df'], cache['merged_df'], cache['product_options'],
     cache['inventory_details_df'], cache['est_stock_days_on_hand_min'], 
     cache['est_stock_days_on_hand_max']) = load_data()
    cache['data_loaded'] = True

# Fetch data once and cache it
def load_data():
    # Fetch all inventory data for the latest partition_date
    inventory_query = """
    SELECT *
    FROM shipbob_inventory_run_rate
    WHERE partition_date = (SELECT MAX(partition_date) FROM shipbob_inventory_run_rate)
    """
    inventory_run_rate_df = run_athena_query(query=inventory_query, database=GLUE_DATABASE, region=REGION,
                                             s3_bucket=S3_BUCKET)

    # Fetch order details data for the past 90 days
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
    order_details_df = run_athena_query(query=order_details_query, database=GLUE_DATABASE, region=REGION,
                                        s3_bucket=S3_BUCKET)

    # Convert dates to datetime and data types
    order_details_df['created_date'] = pd.to_datetime(order_details_df['created_date'])
    order_details_df['inventory_qty'] = pd.to_numeric(order_details_df['inventory_qty'])
    order_details_df['inventory_id'] = pd.to_numeric(order_details_df['inventory_id'])

    # Calculate quantities sold in the last 30, 60, and 90 days
    today = pd.Timestamp.today().normalize()
    date_30_days_ago = today - pd.Timedelta(days=30)
    date_60_days_ago = today - pd.Timedelta(days=60)
    date_90_days_ago = today - pd.Timedelta(days=90)

    # Group by inventory_id and calculate sums
    sales_last_30_days = order_details_df[order_details_df['created_date'] >= date_30_days_ago]
    sales_last_30_days = sales_last_30_days.groupby('inventory_id')['inventory_qty'].sum().reset_index()
    sales_last_30_days.rename(columns={'inventory_qty': 'actual_qty_sold_last_30_days'}, inplace=True)

    sales_last_60_days = order_details_df[order_details_df['created_date'] >= date_60_days_ago]
    sales_last_60_days = sales_last_60_days.groupby('inventory_id')['inventory_qty'].sum().reset_index()
    sales_last_60_days.rename(columns={'inventory_qty': 'actual_qty_sold_last_60_days'}, inplace=True)

    sales_last_90_days = order_details_df[order_details_df['created_date'] >= date_90_days_ago]
    sales_last_90_days = sales_last_90_days.groupby('inventory_id')['inventory_qty'].sum().reset_index()
    sales_last_90_days.rename(columns={'inventory_qty': 'actual_qty_sold_last_90_days'}, inplace=True)

    # Merge sales data into inventory_run_rate_df
    inventory_run_rate_df['inventory_id'] = pd.to_numeric(inventory_run_rate_df['inventory_id'])
    inventory_run_rate_df = inventory_run_rate_df.merge(sales_last_30_days, on='inventory_id', how='left')
    inventory_run_rate_df = inventory_run_rate_df.merge(sales_last_60_days, on='inventory_id', how='left')
    inventory_run_rate_df = inventory_run_rate_df.merge(sales_last_90_days, on='inventory_id', how='left')

    # Fill NaN values with 0
    inventory_run_rate_df[['actual_qty_sold_last_30_days',
                           'actual_qty_sold_last_60_days',
                           'actual_qty_sold_last_90_days']] = inventory_run_rate_df[[
                               'actual_qty_sold_last_30_days',
                               'actual_qty_sold_last_60_days',
                               'actual_qty_sold_last_90_days'
                           ]].fillna(0)

    # Convert sales columns to integers
    inventory_run_rate_df[['actual_qty_sold_last_30_days',
                           'actual_qty_sold_last_60_days',
                           'actual_qty_sold_last_90_days']] = inventory_run_rate_df[[
                               'actual_qty_sold_last_30_days',
                               'actual_qty_sold_last_60_days',
                               'actual_qty_sold_last_90_days'
                           ]].astype(int)

    # Fetch inventory details data for the past 90 days
    inventory_details_query = """
    SELECT id AS inventory_id, partition_date, total_fulfillable_quantity
    FROM shipbob_inventory_details
    WHERE partition_date >= date_add('day', -90, current_date)
    """
    inventory_details_df = run_athena_query(query=inventory_details_query, database=GLUE_DATABASE, region=REGION,
                                            s3_bucket=S3_BUCKET)

    # Convert dates to datetime and data types
    inventory_details_df['partition_date'] = pd.to_datetime(inventory_details_df['partition_date'])
    inventory_details_df['total_fulfillable_quantity'] = pd.to_numeric(inventory_details_df['total_fulfillable_quantity'])
    inventory_details_df['inventory_id'] = pd.to_numeric(inventory_details_df['inventory_id'])

    # Merge inventory_details_df with inventory_run_rate_df to get 'name' for each inventory_id
    inventory_details_df = pd.merge(
        inventory_details_df,
        inventory_run_rate_df[['inventory_id', 'name']],
        on='inventory_id',
        how='left'
    )

    # Now, merge order_details_df with inventory_run_rate_df to get 'run_rate', 'estimated_stockout_date', and 'restock_point'
    merged_df = pd.merge(
        order_details_df,
        inventory_run_rate_df[['inventory_id', 'run_rate', 'estimated_stockout_date', 'restock_point', 'name']],
        on='inventory_id',
        how='left'
    )

    # Convert data types in merged_df
    merged_df['created_date'] = pd.to_datetime(merged_df['created_date'])
    merged_df['inventory_qty'] = pd.to_numeric(merged_df['inventory_qty'])
    merged_df['run_rate'] = pd.to_numeric(merged_df['run_rate'])
    merged_df['estimated_stockout_date'] = pd.to_datetime(merged_df['estimated_stockout_date'])
    merged_df['restock_point'] = pd.to_numeric(merged_df['restock_point'])

    # Convert 'est_stock_days_on_hand' to numeric and update the DataFrame
    inventory_run_rate_df['est_stock_days_on_hand'] = pd.to_numeric(
        inventory_run_rate_df['est_stock_days_on_hand'], errors='coerce'
    )

    # Drop rows where 'est_stock_days_on_hand' is NaN
    inventory_run_rate_df = inventory_run_rate_df.dropna(subset=['est_stock_days_on_hand'])

    # Convert to integers and round
    inventory_run_rate_df['est_stock_days_on_hand'] = inventory_run_rate_df['est_stock_days_on_hand'].astype(float).round().astype(int)

    # Now, compute min and max using the updated DataFrame
    est_stock_days_on_hand_min = inventory_run_rate_df['est_stock_days_on_hand'].min()
    est_stock_days_on_hand_max = inventory_run_rate_df['est_stock_days_on_hand'].max()

    # Fetch distinct product names for the dropdown
    product_options = [{'label': name, 'value': name} for name in sorted(inventory_run_rate_df['name'].unique())]

    return (inventory_run_rate_df, merged_df, product_options, inventory_details_df,
            est_stock_days_on_hand_min, est_stock_days_on_hand_max)


# Define the legend data
def get_legend_data():
    legend_items = [
        {"Value": "Shipbob Inventory ID", "Description": "The Shipbob 'Inventory ID' value, a unique ID for each physical product on ShipBob.", "Color": "#1f77b4"},
        {"Value": "Name", "Description": "The name of the physical product on ShipBob.", "Color": "#ff7f0e"},
        {"Value": "Daily Run Rate", "Description": "The daily run rate of this item, calculated using actual qty sold from ShipBob order data.", "Color": "#2ca02c"},
        {"Value": "Total Fulfillable Quantity", "Description": "The total fulfillable quantity of the physical product on hand per ShipBob.", "Color": "#d62728"},
        {"Value": "Restock Point", "Description": "The inventory level when a MO for this product should be submitted to ensure sufficient stock.", "Color": "#9467bd"},
        {"Value": "Est. Stock Days On Hand", "Description": "The estimated remaining days this product will be in stock.", "Color": "#8c564b"},
        {"Value": "Est. Stockout Date", "Description": "The estimated stockout date based on current run rate and inventory.", "Color": "#e377c2"},
        {"Value": "Actual Qty. Sold - Past 30 Days", "Description": "The actual quantity of this product sold in the past 30 days.", "Color": "#7f7f7f"},
        {"Value": "Actual Qty. Sold - Past 60 Days", "Description": "The actual quantity of this product sold in the past 60 days.", "Color": "#bcbd22"},
        {"Value": "Actual Qty. Sold - Past 90 Days", "Description": "The actual quantity of this product sold in the past 90 days.", "Color": "#17becf"},
    ]

    # Create DataFrame
    legend_df = pd.DataFrame(legend_items)
    legend_df['Value'] = legend_df['Value'].apply(lambda x: f'\u25A0 {x}')  # Add square

    return legend_df

# Updated get_color function based on the specified ranges
def get_color(value):
    if value < 60:
        return 'darkred'
    elif 60 <= value < 70:
        return 'red'
    elif 70 <= value <= 100:
        return 'orange'
    else:  # value > 100
        return 'green'

# Define the app layout with tabs and main content
app.layout = html.Div([
    # Store components to keep track of hidden cards and selected product
    dcc.Store(id='hidden-cards-store', data=[]),
    dcc.Store(id='selected-product-store', data=None),
    # Tabs
    dcc.Tabs(id='tabs', value='product-cards', children=[
        # Product Cards Tab
        dcc.Tab(label='Product Cards', value='product-cards', children=[
            html.Div([
                html.H1('Estimated Stock Days On Hand by Product', style={'textAlign': 'center', 'color': '#003366', 'marginTop': '20px'}),

                # Add Legend
                html.Div([
                    html.H4('Color Legend:', style={'textAlign': 'center', 'color': '#003366', 'marginTop': '20px'}),
                    html.Ul([
                        html.Li([
                            html.Span(style={'backgroundColor': 'darkred', 'display': 'inline-block', 'width': '20px', 'height': '20px', 'marginRight': '10px'}),
                            "< 60 days of stock on hand. An MO for this product should already exist, if not one needs to be created ASAP to avoid potential stockouts due to raw material lead times."
                        ], style={'marginBottom': '10px'}),
                        html.Li([
                            html.Span(style={'backgroundColor': 'red', 'display': 'inline-block', 'width': '20px', 'height': '20px', 'marginRight': '10px'}),
                            "60 - 70 days of stock on hand. An MO for this product should already exist, if not one needs to be created ASAP to avoid potential stockouts due to raw material lead times."
                        ], style={'marginBottom': '10px'}),
                        html.Li([
                            html.Span(style={'backgroundColor': 'orange', 'display': 'inline-block', 'width': '20px', 'height': '20px', 'marginRight': '10px'}),
                            "70 - 100 days of stock on hand. Approaching the need to submit a MO to replenish inventory for this product."
                        ], style={'marginBottom': '10px'}),
                        html.Li([
                            html.Span(style={'backgroundColor': 'green', 'display': 'inline-block', 'width': '20px', 'height': '20px', 'marginRight': '10px'}),
                            "> 100 days of stock on hand, at least 30 days out from needing to submit a MO for replenishment."
                        ])
                    ], style={'listStyleType': 'none'})
                ], style={'margin': '20px'}),

                html.Button(
                    "Reset Hidden Cards",
                    id="reset-hidden-cards",
                    n_clicks=0,
                    style={
                        'backgroundColor': '#1f77b4',
                        'color': 'white',
                        'padding': '10px 20px',
                        'border': 'none',
                        'borderRadius': '5px',
                        'cursor': 'pointer',
                        'marginBottom': '20px',
                        'display': 'block',
                        'marginLeft': 'auto',
                        'marginRight': 'auto'
                    }
                ),
                # Slider for Est. Stock Days On Hand
                html.Div([
                    html.Label('Select Est. Stock Days On Hand Range:', style={'fontWeight': 'bold'}),
                    dcc.RangeSlider(
                        id='est-stock-days-slider',
                        min=int(float(cache['est_stock_days_on_hand_min'])),
                        max=int(float(cache['est_stock_days_on_hand_max'])),
                        value=[int(float(cache['est_stock_days_on_hand_min'])), int(float(cache['est_stock_days_on_hand_max']))],
                        marks={i: str(i) for i in range(int(float(cache['est_stock_days_on_hand_min'])), int(float(cache['est_stock_days_on_hand_max']))+1, 10)},
                        step=1,
                        allowCross=False
                    )
                ], style={'margin': '20px'}),
                html.Div(
                    id='kpi-cards-container',
                    style={
                        'display': 'flex',
                        'flexWrap': 'wrap',
                        'justifyContent': 'center',
                        'marginTop': '20px'
                    }
                )
            ], style={'padding': '20px'})
        ]),

        # Product Summary Tab
        dcc.Tab(label='Product Summary', value='product-summary', children=[
            # Sidebar
            html.Div([
                html.H2('Product Selection', style={'textAlign': 'center', 'color': '#ffffff'}),
                dcc.Dropdown(
                    id='product-dropdown',
                    options=cache['product_options'] if cache['product_options'] else [],
                    placeholder='Select an Inventory Item',
                    style={
                        'margin': '20px',
                        'color': '#000000',
                        'backgroundColor': '#ffffff',
                        'borderColor': '#ffffff'
                    }
                ),

                # Table Legend in Sidebar
                html.Div([
                    html.H4('Table Legend:', style={'marginTop': '30px', 'color': '#ffffff', 'textAlign': 'center'}),
                    dash_table.DataTable(
                        id='legend-table',
                        columns=[
                            {'name': 'Value', 'id': 'Value'},
                            {'name': 'Description', 'id': 'Description'},
                        ],
                        data=get_legend_data().to_dict('records'),
                        style_table={
                            'width': '100%',
                            'backgroundColor': '#333333',
                        },
                        style_header={
                            'backgroundColor': '#555555',
                            'color': 'white',
                            'fontWeight': 'bold',
                            'textAlign': 'left'
                        },
                        style_cell={
                            'padding': '5px',
                            'backgroundColor': '#333333',
                            'color': 'white',
                            'border': '1px solid #444444',
                            'textAlign': 'left',
                            'whiteSpace': 'normal',
                            'height': 'auto',
                        },
                        style_data_conditional=[
                            {
                                'if': {'column_id': 'Value'},
                                'width': '200px'
                            },
                            {
                                'if': {'column_id': 'Description'},
                                'width': '400px'
                            },
                            # Apply colored squares using conditional styling
                            {
                                'if': {'filter_query': '{Value} = "■ Shipbob Inventory ID"', 'column_id': 'Value'},
                                'color': '#1f77b4'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Name"', 'column_id': 'Value'},
                                'color': '#ff7f0e'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Daily Run Rate"', 'column_id': 'Value'},
                                'color': '#2ca02c'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Total Fulfillable Quantity"', 'column_id': 'Value'},
                                'color': '#d62728'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Restock Point"', 'column_id': 'Value'},
                                'color': '#9467bd'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Est. Stock Days On Hand"', 'column_id': 'Value'},
                                'color': '#8c564b'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Est. Stockout Date"', 'column_id': 'Value'},
                                'color': '#e377c2'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Actual Qty. Sold - Past 30 Days"', 'column_id': 'Value'},
                                'color': '#7f7f7f'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Actual Qty. Sold - Past 60 Days"', 'column_id': 'Value'},
                                'color': '#bcbd22'
                            },
                            {
                                'if': {'filter_query': '{Value} = "■ Actual Qty. Sold - Past 90 Days"', 'column_id': 'Value'},
                                'color': '#17becf'
                            },
                        ],
                        sort_action='none',
                        filter_action='none',
                        page_action='none',
                        style_as_list_view=True,
                    ),
                ], style={
                    'backgroundColor': '#333333',
                    'padding': '10px 20px',
                    'borderRadius': '5px',
                    'marginTop': '20px'
                }),
            ], style={
                'position': 'fixed',
                'top': 50,
                'left': 0,
                'bottom': 0,
                'width': '20%',
                'padding': '20px',
                'backgroundColor': '#333333',
                'color': '#ffffff',
                'overflowY': 'auto'
            }),

            # Main Content
            html.Div([
                html.H1('Prymal Inventory Dashboard', style={'textAlign': 'center', 'color': '#003366'}),

                # Collapsible Stat Cards
                html.Div([
                    html.Button(
                        "Toggle Stat Cards",
                        id="toggle-stat-cards",
                        n_clicks=0,
                        style={
                            'backgroundColor': '#1f77b4',
                            'color': 'white',
                            'padding': '10px 20px',
                            'border': 'none',
                            'borderRadius': '5px',
                            'cursor': 'pointer',
                            'marginBottom': '10px'
                        }
                    ),
                    html.Div([
                        # Quantity in Stock
                        html.Div([
                            html.H4('Quantity in Stock', style={'textAlign': 'center', 'color': '#1f77b4'}),
                            html.H2(id='quantity-in-stock', style={'textAlign': 'center'}, title='Total number of items currently in stock.')
                        ], style={
                            'border': '1px solid #ddd',
                            'borderRadius': '5px',
                            'padding': '20px',
                            'width': '23%',
                            'backgroundColor': '#f0f8ff',
                            'boxShadow': '2px 2px 5px rgba(0,0,0,0.1)',
                            'cursor': 'pointer',
                            'margin': '10px'
                        }),

                        # Est. Stock Days On Hand
                        html.Div([
                            html.H4('Est. Stock Days On Hand', style={'textAlign': 'center', 'color': '#ff7f0e'}),
                            html.H2(id='est-stock-days-on-hand', style={'textAlign': 'center'}, title='Estimated number of days the current stock will last based on sales.')
                        ], style={
                            'border': '1px solid #ddd',
                            'borderRadius': '5px',
                            'padding': '20px',
                            'width': '23%',
                            'backgroundColor': '#fffaf0',
                            'boxShadow': '2px 2px 5px rgba(0,0,0,0.1)',
                            'cursor': 'pointer',
                            'margin': '10px'
                        }),

                        # Est. Stockout Date
                        html.Div([
                            html.H4('Est. Stockout Date', style={'textAlign': 'center', 'color': '#2ca02c'}),
                            html.H2(id='est-stockout-date', style={'textAlign': 'center'}, title='Projected date when the current stock will be depleted.')
                        ], style={
                            'border': '1px solid #ddd',
                            'borderRadius': '5px',
                            'padding': '20px',
                            'width': '23%',
                            'backgroundColor': '#f5fff5',
                            'boxShadow': '2px 2px 5px rgba(0,0,0,0.1)',
                            'cursor': 'pointer',
                            'margin': '10px'
                        }),

                        # Daily Run Rate
                        html.Div([
                            html.H4('Daily Run Rate', style={'textAlign': 'center', 'color': '#d62728'}),
                            html.H2(id='daily-run-rate', style={'textAlign': 'center'}, title='Average number of units sold per day.')
                        ], style={
                            'border': '1px solid #ddd',
                            'borderRadius': '5px',
                            'padding': '20px',
                            'width': '23%',
                            'backgroundColor': '#fff0f5',
                            'boxShadow': '2px 2px 5px rgba(0,0,0,0.1)',
                            'cursor': 'pointer',
                            'margin': '10px'
                        }),
                    ], style={
                        'display': 'flex',
                        'justifyContent': 'space-around',
                        'flexWrap': 'wrap'
                    }, id='stat-cards-container')
                ], id='collapsible-stat-cards'),

                # Collapsible Inventory Status Table
                html.Div([
                    html.Button(
                        "Toggle Inventory Status Table",
                        id="toggle-inventory-table",
                        n_clicks=0,
                        style={
                            'backgroundColor': '#2ca02c',
                            'color': 'white',
                            'padding': '10px 20px',
                            'border': 'none',
                            'borderRadius': '5px',
                            'cursor': 'pointer',
                            'marginTop': '20px',
                            'marginBottom': '10px'
                        }
                    ),
                    html.Div([
                        html.H2('Current Inventory Status', style={'color': '#003366', 'textAlign': 'center'}),

                        # Interactive DataTable Placeholder
                        html.Div(
                            id='inventory-status-table',
                            style={'border': '1px solid #ddd', 'borderRadius': '5px',
                                   'padding': '10px', 'backgroundColor': '#ffffff'}
                        ),
                    ], id='inventory-table-container', style={'display': 'block'})
                ], id='collapsible-inventory-table'),

                # Collapsible Time Series Graphs
                html.Div([
                    html.Button(
                        "Toggle Time Series Graphs",
                        id="toggle-time-series",
                        n_clicks=0,
                        style={
                            'backgroundColor': '#ff7f0e',
                            'color': 'white',
                            'padding': '10px 20px',
                            'border': 'none',
                            'borderRadius': '5px',
                            'cursor': 'pointer',
                            'marginTop': '20px',
                            'marginBottom': '10px'
                        }
                    ),
                    html.Div([
                        html.H2('Actual Qty. Sold & Current Daily Run Rate',
                                style={'color': '#003366', 'textAlign': 'center'}),
                        dcc.Graph(id='qty-sold-time-series'),
                        html.H2('Inventory On Hand Trend',
                                style={'color': '#003366', 'textAlign': 'center', 'marginTop': '50px'}),
                        dcc.Graph(id='inventory-on-hand-time-series'),
                    ], id='time-series-container', style={'display': 'block'})
                ], id='collapsible-time-series'),

            ], style={
                'marginLeft': '22%',  # To accommodate the fixed sidebar
                'padding': '20px'
            }),
        ]),

    ])
])

# Callback to toggle Stat Cards visibility
@app.callback(
    Output("stat-cards-container", "style"),
    Input("toggle-stat-cards", "n_clicks"),
    State("stat-cards-container", "style")
)
def toggle_stat_cards(n_clicks, current_style):
    if current_style is None:
        current_style = {'display': 'flex'}
    if n_clicks % 2 == 1:
        return {**current_style, 'display': 'none'}
    else:
        return {**current_style, 'display': 'flex'}

# Callback to toggle Inventory Status Table visibility
@app.callback(
    Output("inventory-table-container", "style"),
    Input("toggle-inventory-table", "n_clicks"),
    State("inventory-table-container", "style")
)
def toggle_inventory_table(n_clicks, current_style):
    if current_style is None:
        current_style = {'display': 'block'}
    if n_clicks % 2 == 1:
        return {**current_style, 'display': 'none'}
    else:
        return {**current_style, 'display': 'block'}

# Callback to toggle Time Series Graph visibility
@app.callback(
    Output("time-series-container", "style"),
    Input("toggle-time-series", "n_clicks"),
    State("time-series-container", "style")
)
def toggle_time_series(n_clicks, current_style):
    if current_style is None:
        current_style = {'display': 'block'}
    if n_clicks % 2 == 1:
        return {**current_style, 'display': 'none'}
    else:
        return {**current_style, 'display': 'block'}

# Main Callback to update Stat Cards, Inventory Table, and Time Series Graphs
@app.callback(
    [Output('quantity-in-stock', 'children'),
     Output('est-stock-days-on-hand', 'children'),
     Output('est-stockout-date', 'children'),
     Output('daily-run-rate', 'children'),
     Output('inventory-status-table', 'children'),
     Output('qty-sold-time-series', 'figure'),
     Output('inventory-on-hand-time-series', 'figure')],
    [Input('product-dropdown', 'value')]
)
def update_dashboard(selected_product):
    # Use the cached data
    if not selected_product:
        # Default values or empty indicators
        return ("-", "-", "-", "-",
                html.Div(['Please select a product to view its inventory status and sales data.']),
                go.Figure(), go.Figure())

    # Filter inventory data for the selected product
    inventory_run_rate_df = cache['inventory_run_rate_df'][cache['inventory_run_rate_df']['name'] == selected_product].copy()

    if inventory_run_rate_df.empty:
        # Indicators as "-"
        return ("-", "-", "-", "-",
                html.Div(['No inventory data available for the selected product.']),
                go.Figure(), go.Figure())

    else:
        # Convert inventory data types appropriately
        inventory_run_rate_df.loc[:, 'restock_point'] = inventory_run_rate_df['restock_point'].astype(float).round().astype('int64')
        inventory_run_rate_df.loc[:, 'est_stock_days_on_hand'] = inventory_run_rate_df['est_stock_days_on_hand'].astype(float).round().astype('int64')
        inventory_run_rate_df.loc[:, 'run_rate'] = inventory_run_rate_df['run_rate'].astype(float).round().astype('int64')  # Changed to int

        inventory_run_rate_df = inventory_run_rate_df.astype({
            'inventory_id': 'int64',
            'total_fulfillable_quantity': 'int64',
            'est_stock_days_on_hand': 'int64',
            'estimated_stockout_date': 'datetime64[ns]',
            'restock_point': 'int64',
            'actual_qty_sold_last_30_days': 'int64',
            'actual_qty_sold_last_60_days': 'int64',
            'actual_qty_sold_last_90_days': 'int64'
        })

        # Extract metrics for stat cards
        quantity_in_stock = inventory_run_rate_df['total_fulfillable_quantity'].iloc[0]
        est_stock_days_on_hand = inventory_run_rate_df['est_stock_days_on_hand'].iloc[0]
        est_stockout_date = inventory_run_rate_df['estimated_stockout_date'].iloc[0]
        daily_run_rate = inventory_run_rate_df['run_rate'].iloc[0]

        # Format the estimated stockout date
        if pd.notnull(est_stockout_date):
            est_stockout_date_formatted = est_stockout_date.strftime('%Y-%m-%d')
        else:
            est_stockout_date_formatted = "N/A"

        # Reorder columns if desired
        columns_order = ['inventory_id', 'name', 'run_rate', 'total_fulfillable_quantity', 'restock_point',
                         'est_stock_days_on_hand', 'estimated_stockout_date',
                         'actual_qty_sold_last_30_days', 'actual_qty_sold_last_60_days', 'actual_qty_sold_last_90_days']
        inventory_run_rate_df = inventory_run_rate_df[columns_order]

        # Create interactive DataTable
        inventory_table = dash_table.DataTable(
            data=inventory_run_rate_df.to_dict('records'),
            columns=[
                {'name': 'Shipbob Inventory ID', 'id': 'inventory_id'},
                {'name': 'Name', 'id': 'name'},
                {'name': 'Daily Run Rate', 'id': 'run_rate'},
                {'name': 'Total Fulfillable Quantity', 'id': 'total_fulfillable_quantity'},
                {'name': 'Restock Point', 'id': 'restock_point'},
                {'name': 'Est. Stock Days On Hand', 'id': 'est_stock_days_on_hand'},
                {'name': 'Est. Stockout Date', 'id': 'estimated_stockout_date'},
                {'name': 'Actual Qty. Sold - Past 30 Days', 'id': 'actual_qty_sold_last_30_days'},
                {'name': 'Actual Qty. Sold - Past 60 Days', 'id': 'actual_qty_sold_last_60_days'},
                {'name': 'Actual Qty. Sold - Past 90 Days', 'id': 'actual_qty_sold_last_90_days'},
            ],
            style_table={'overflowX': 'auto'},
            style_cell={
                'minWidth': '150px', 'width': '180px', 'maxWidth': '200px',
                'whiteSpace': 'normal',
                'textAlign': 'left',
                'padding': '5px',
                'color': 'black',
                'backgroundColor': 'white'
            },
            style_header={
                'backgroundColor': '#003366',
                'color': 'white',
                'fontWeight': 'bold',
                'textAlign': 'center'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f2f2f2'
                }
            ],
            page_size=10,
            sort_action='native',
            filter_action='native',
            row_selectable='multi',
            tooltip_delay=0,
            tooltip_duration=None,
        )

    # Filter merged data for the selected product
    product_df = cache['merged_df'][cache['merged_df']['name'] == selected_product]

    # Filter inventory_details_df for the selected product
    inventory_details_df = cache['inventory_details_df'][cache['inventory_details_df']['name'] == selected_product]

    # Ensure 'restock_point' is defined before using it
    try:
        restock_point = int(inventory_run_rate_df['restock_point'].iloc[0])
    except (KeyError, IndexError, ValueError) as e:
        restock_point = 0  # Assign a default value or handle as needed
        print(f"Error defining restock_point: {e}")

    # Ensure 'daily_run_rate' is defined
    try:
        daily_run_rate = int(inventory_run_rate_df['run_rate'].iloc[0])
    except (KeyError, IndexError, ValueError) as e:
        daily_run_rate = 0  # Assign a default value or handle as needed
        print(f"Error defining daily_run_rate: {e}")

    # Create fig1: Actual Quantity Sold and Run Rate
    if product_df.empty:
        fig1 = go.Figure()
        fig1.add_annotation(text="No sales data available for the selected product.",
                            xref="paper", yref="paper",
                            showarrow=False, font=dict(size=20))
    else:
        fig1 = go.Figure()

        # Actual quantity sold
        fig1.add_trace(go.Scatter(
            x=product_df['created_date'],
            y=product_df['inventory_qty'],
            mode='lines+markers',
            name='Actual Quantity Sold',
            line=dict(color='#1f77b4'),
            marker=dict(color='#1f77b4')
        ))

        # Add 'Daily Run Rate' as a horizontal line using Scatter
        fig1.add_trace(go.Scatter(
            x=[product_df['created_date'].min(), product_df['created_date'].max()],
            y=[daily_run_rate, daily_run_rate],
            mode='lines',
            name='Daily Run Rate',
            line=dict(color='#ff7f0e', dash='dash'),
        ))

        fig1.update_layout(
            title='Actual Qty. Sold & Current Daily Run Rate',
            title_font_size=20,
            xaxis_title='Date',
            yaxis=dict(
                title='Quantity',
                autorange=True
            ),
            legend_title='Legend',
            legend=dict(
                x=1.02,
                y=1,
                traceorder='normal',
                font=dict(size=12),
                bgcolor='rgba(0,0,0,0)',
                bordercolor='rgba(0,0,0,0)'
            ),
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=7, label='1w', step='day', stepmode='backward'),
                        dict(count=1, label='1m', step='month', stepmode='backward'),
                        dict(count=3, label='3m', step='month', stepmode='backward'),
                        dict(step='all')
                    ])
                ),
                rangeslider=dict(visible=True),
                type='date'
            ),
            plot_bgcolor='#f9f9f9',
            paper_bgcolor='#ffffff',
            font=dict(color='#333333'),
            margin=dict(l=50, r=150, t=50, b=50)
        )

    # Create fig2: Inventory On Hand and Restock Point
    if inventory_details_df.empty:
        fig2 = go.Figure()
        fig2.add_annotation(text="No inventory data available for the selected product.",
                            xref="paper", yref="paper",
                            showarrow=False, font=dict(size=20))
    else:
        fig2 = go.Figure()

        # Inventory on hand over time
        inventory_details_df = inventory_details_df.sort_values('partition_date')
        fig2.add_trace(go.Scatter(
            x=inventory_details_df['partition_date'],
            y=inventory_details_df['total_fulfillable_quantity'],
            mode='lines+markers',
            name='Inventory On Hand',
            line=dict(color='#2ca02c'),
            marker=dict(color='#2ca02c')
        ))

        # Restock Point as a horizontal line using Scatter
        fig2.add_trace(go.Scatter(
            x=[inventory_details_df['partition_date'].min(), inventory_details_df['partition_date'].max()],
            y=[restock_point, restock_point],
            mode='lines',
            name='Restock Point',
            line=dict(color='green', dash='dot'),
        ))

        # Estimated Stockout Date vertical line as a Scatter trace
        est_stockout_date = inventory_run_rate_df['estimated_stockout_date'].iloc[0]

        if pd.notnull(est_stockout_date):
            est_stockout_date = pd.to_datetime(est_stockout_date)

            if est_stockout_date:
                ymin = inventory_details_df['total_fulfillable_quantity'].min() * 0.95
                ymax = inventory_details_df['total_fulfillable_quantity'].max() * 1.05

                fig2.add_trace(go.Scatter(
                    x=[est_stockout_date, est_stockout_date],
                    y=[ymin, ymax],
                    mode='lines',
                    name='Estimated Stockout Date',
                    line=dict(color='red', dash='dot'),
                ))

        fig2.update_layout(
            title='Inventory On Hand Trend',
            title_font_size=20,
            xaxis_title='Date',
            yaxis=dict(
                title='Quantity',
                autorange=True
            ),
            legend_title='Legend',
            legend=dict(
                x=1.02,
                y=1,
                traceorder='normal',
                font=dict(size=12),
                bgcolor='rgba(0,0,0,0)',
                bordercolor='rgba(0,0,0,0)'
            ),
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=7, label='1w', step='day', stepmode='backward'),
                        dict(count=1, label='1m', step='month', stepmode='backward'),
                        dict(count=3, label='3m', step='month', stepmode='backward'),
                        dict(step='all')
                    ])
                ),
                rangeslider=dict(visible=True),
                type='date'
            ),
            plot_bgcolor='#f9f9f9',
            paper_bgcolor='#ffffff',
            font=dict(color='#333333'),
            margin=dict(l=50, r=150, t=50, b=50)
        )

    return (quantity_in_stock, est_stock_days_on_hand, est_stockout_date_formatted, daily_run_rate,
            inventory_table, fig1, fig2)

# Callback to create KPI cards on the Product Cards Page
@app.callback(
    Output('kpi-cards-container', 'children'),
    [Input('tabs', 'value'),
     Input('hidden-cards-store', 'data'),
     Input('est-stock-days-slider', 'value')],
)
def update_kpi_cards(selected_tab, hidden_cards, est_stock_days_range):
    if selected_tab != 'product-cards':
        # Return empty list if not on the Product Cards Page
        return []

    # Use the cached data
    df = cache['inventory_run_rate_df'][['name', 'est_stock_days_on_hand']].copy()

    # Convert est_stock_days_on_hand to numeric, handling any errors
    df['est_stock_days_on_hand'] = pd.to_numeric(df['est_stock_days_on_hand'], errors='coerce')
    
    # Drop any rows where est_stock_days_on_hand is NaN
    df = df.dropna(subset=['est_stock_days_on_hand'])
    
    # Filter based on est_stock_days_range
    min_range, max_range = est_stock_days_range
    df = df[(df['est_stock_days_on_hand'] >= min_range) & (df['est_stock_days_on_hand'] <= max_range)]
    
    # Convert to integer for display
    df['est_stock_days_on_hand'] = df['est_stock_days_on_hand'].astype(int)

    # Sort the DataFrame in ascending order
    df = df.sort_values(by='est_stock_days_on_hand', ascending=True)

    # Filter out hidden cards
    if hidden_cards:
        df = df[~df['name'].isin(hidden_cards)]

    # Create cards
    cards = []
    for index, row in df.iterrows():
        value = row['est_stock_days_on_hand']
        color = get_color(value)
        card = html.Div([
            # Hide button
            html.Button('X', id={'type': 'hide-button', 'index': row['name']}, n_clicks=0, style={
                'position': 'absolute',
                'top': '5px',
                'right': '5px',
                'backgroundColor': 'transparent',
                'border': 'none',
                'color': 'white',
                'fontSize': '16px',
                'cursor': 'pointer'
            }),
            # Card content
            html.Div([
                html.H4(row['name'], style={'textAlign': 'center', 'color': '#ffffff', 'fontSize': '16px', 'marginTop': '20px'}),
                html.H2(f"{value}", style={'textAlign': 'center', 'color': '#ffffff', 'fontSize': '24px'})
            ], style={'cursor': 'pointer'}, id={'type': 'product-card', 'index': row['name']}, n_clicks=0)
        ], style={
            'position': 'relative',
            'border': '1px solid #ddd',
            'borderRadius': '5px',
            'padding': '10px',
            'width': '200px',
            'height': '150px',
            'margin': '10px',
            'backgroundColor': color,
            'boxShadow': '2px 2px 5px rgba(0,0,0,0.1)',
            'overflow': 'hidden'
        })
        cards.append(card)

    return cards

# Callback to handle hiding individual cards
@app.callback(
    Output('hidden-cards-store', 'data'),
    [Input({'type': 'hide-button', 'index': ALL}, 'n_clicks'),
     Input('reset-hidden-cards', 'n_clicks')],
    [State({'type': 'hide-button', 'index': ALL}, 'id'),
     State('hidden-cards-store', 'data')]
)
def hide_card(n_clicks_list, reset_n_clicks, ids, hidden_cards):
    ctx = dash.callback_context

    if hidden_cards is None:
        hidden_cards = []

    # Check if reset button was clicked
    if ctx.triggered and 'reset-hidden-cards' in ctx.triggered[0]['prop_id']:
        return []

    # Find which button was clicked
    for n_clicks, id_dict in zip(n_clicks_list, ids):
        if n_clicks and id_dict['index'] not in hidden_cards:
            hidden_cards.append(id_dict['index'])

    return hidden_cards

# Callback to handle card clicks and navigate to Product Summary Page
@app.callback(
    [Output('tabs', 'value'),
     Output('selected-product-store', 'data')],
    [Input({'type': 'product-card', 'index': ALL}, 'n_clicks')],
    [State({'type': 'product-card', 'index': ALL}, 'id'),
     State({'type': 'product-card', 'index': ALL}, 'n_clicks_timestamp')],
)
def on_card_click(n_clicks_list, ids, n_clicks_timestamps):
    ctx = dash.callback_context

    if not ctx.triggered:
        return dash.no_update, dash.no_update

    if not n_clicks_timestamps:
        return dash.no_update, dash.no_update

    # Filter out None timestamps
    valid_timestamps = [ts for ts in n_clicks_timestamps if ts is not None]

    if not valid_timestamps:
        return dash.no_update, dash.no_update

    # Find the index of the card with the latest click
    max_timestamp = max(valid_timestamps)
    max_index = n_clicks_timestamps.index(max_timestamp)
    card_id = ids[max_index]['index']

    return 'product-summary', card_id

# Callback to update the product dropdown when a card is clicked
@app.callback(
    Output('product-dropdown', 'value'),
    Input('selected-product-store', 'data')
)
def update_product_dropdown(selected_product):
    if selected_product:
        return selected_product
    return dash.no_update

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
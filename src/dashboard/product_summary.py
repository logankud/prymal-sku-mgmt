from dash import dcc, html, Input, Output, State, dash_table, callback
import plotly.graph_objs as go
import pandas as pd
from loguru import logger


###############################################################################
#                   HELPER FUNCTIONS / ANY UTILITIES FOR THIS TAB
###############################################################################
def get_legend_data():
    """
    Same logic as in your original code,
    returning a DataFrame for the table legend.
    """
    legend_items = [
        {
            "Value": "Shipbob Inventory ID",
            "Description": "Unique ID on ShipBob.",
            "Color": "#1f77b4"
        },
        {
            "Value": "Name",
            "Description": "Name of physical product on ShipBob.",
            "Color": "#ff7f0e"
        },
        {
            "Value": "Daily Run Rate",
            "Description": "Avg daily run rate of this item from orders.",
            "Color": "#2ca02c"
        },
        {
            "Value": "Total Fulfillable Quantity",
            "Description": "Total fulfillable quantity on hand.",
            "Color": "#d62728"
        },
        {
            "Value": "Restock Point",
            "Description": "Level at which a MO is triggered.",
            "Color": "#9467bd"
        },
        {
            "Value": "Est. Stock Days On Hand",
            "Description": "Estimated days of stock left.",
            "Color": "#8c564b"
        },
        {
            "Value": "Est. Stockout Date",
            "Description": "Estimated date stock runs out.",
            "Color": "#e377c2"
        },
        {
            "Value": "Actual Qty. Sold - Past 30 Days",
            "Description": "Qty sold in the last 30 days.",
            "Color": "#7f7f7f"
        },
        {
            "Value": "Actual Qty. Sold - Past 60 Days",
            "Description": "Qty sold in the last 60 days.",
            "Color": "#bcbd22"
        },
        {
            "Value": "Actual Qty. Sold - Past 90 Days",
            "Description": "Qty sold in the last 90 days.",
            "Color": "#17becf"
        },
    ]
    legend_df = pd.DataFrame(legend_items)
    legend_df['Value'] = legend_df['Value'].apply(lambda x: f'\u25A0 {x}')
    return legend_df


###############################################################################
#             LAYOUT FOR THE "PRODUCT SUMMARY" TAB (RETURN dcc.Tab)
###############################################################################
def get_product_summary_tab(product_options):
    """
    Returns the dcc.Tab layout for the 'Product Summary' page.
    """
    return dcc.Tab(
        label='Product Summary',
        value='product-summary',
        children=[
            # Sidebar
            html.Div(
                [
                    html.H2('Product Selection',
                            style={
                                'textAlign': 'center',
                                'color': '#ffffff'
                            }),
                    dcc.Dropdown(
                        id='product-dropdown',
                        options=product_options if product_options else [],
                        placeholder='Select an Inventory Item',
                        style={
                            'margin': '20px',
                            'color': '#000000',
                            'backgroundColor': '#ffffff',
                            'borderColor': '#ffffff'
                        }),

                    # Table Legend in Sidebar
                    html.Div(
                        [
                            html.H4('Table Legend:',
                                    style={
                                        'marginTop': '30px',
                                        'color': '#ffffff',
                                        'textAlign': 'center'
                                    }),
                            dash_table.DataTable(
                                id='legend-table',
                                columns=[
                                    {
                                        'name': 'Value',
                                        'id': 'Value'
                                    },
                                    {
                                        'name': 'Description',
                                        'id': 'Description'
                                    },
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
                                        'if': {
                                            'column_id': 'Value'
                                        },
                                        'width': '200px'
                                    },
                                    {
                                        'if': {
                                            'column_id': 'Description'
                                        },
                                        'width': '400px'
                                    },
                                    # Colored squares
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Shipbob Inventory ID"'
                                        },
                                        'color': '#1f77b4'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Name"'
                                        },
                                        'color': '#ff7f0e'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Daily Run Rate"'
                                        },
                                        'color': '#2ca02c'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Total Fulfillable Quantity"'
                                        },
                                        'color': '#d62728'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Restock Point"'
                                        },
                                        'color': '#9467bd'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Est. Stock Days On Hand"'
                                        },
                                        'color': '#8c564b'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Est. Stockout Date"'
                                        },
                                        'color': '#e377c2'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Actual Qty. Sold - Past 30 Days"'
                                        },
                                        'color': '#7f7f7f'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Actual Qty. Sold - Past 60 Days"'
                                        },
                                        'color': '#bcbd22'
                                    },
                                    {
                                        'if': {
                                            'filter_query':
                                            '{Value} = "■ Actual Qty. Sold - Past 90 Days"'
                                        },
                                        'color': '#17becf'
                                    },
                                ],
                                sort_action='none',
                                filter_action='none',
                                page_action='none',
                                style_as_list_view=True,
                            ),
                        ],
                        style={
                            'backgroundColor': '#333333',
                            'padding': '10px 20px',
                            'borderRadius': '5px',
                            'marginTop': '20px'
                        }),
                ],
                style={
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
            html.Div(
                [
                    html.H1('Prymal Inventory Dashboard',
                            style={
                                'textAlign': 'center',
                                'color': '#003366'
                            }),

                    # Collapsible Stat Cards
                    html.Div(
                        [
                            html.Button("Toggle Stat Cards",
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
                                        }),
                            html.Div(
                                [
                                    # Quantity in Stock
                                    html.Div(
                                        [
                                            html.H4('Quantity in Stock',
                                                    style={
                                                        'textAlign': 'center',
                                                        'color': '#1f77b4'
                                                    }),
                                            html.
                                            H2(id='quantity-in-stock',
                                               style={'textAlign': 'center'},
                                               title=
                                               'Total items currently in stock.'
                                               )
                                        ],
                                        style={
                                            'border': '1px solid #ddd',
                                            'borderRadius': '5px',
                                            'padding': '20px',
                                            'width': '23%',
                                            'backgroundColor': '#f0f8ff',
                                            'boxShadow':
                                            '2px 2px 5px rgba(0,0,0,0.1)',
                                            'cursor': 'pointer',
                                            'margin': '10px'
                                        }),

                                    # Est. Stock Days On Hand
                                    html.Div(
                                        [
                                            html.H4('Est. Stock Days On Hand',
                                                    style={
                                                        'textAlign': 'center',
                                                        'color': '#ff7f0e'
                                                    }),
                                            html.
                                            H2(id='est-stock-days-on-hand',
                                               style={'textAlign': 'center'},
                                               title=
                                               'Estimated # of days stock will last.'
                                               )
                                        ],
                                        style={
                                            'border': '1px solid #ddd',
                                            'borderRadius': '5px',
                                            'padding': '20px',
                                            'width': '23%',
                                            'backgroundColor': '#fffaf0',
                                            'boxShadow':
                                            '2px 2px 5px rgba(0,0,0,0.1)',
                                            'cursor': 'pointer',
                                            'margin': '10px'
                                        }),

                                    # Est. Stockout Date
                                    html.Div(
                                        [
                                            html.H4('Est. Stockout Date',
                                                    style={
                                                        'textAlign': 'center',
                                                        'color': '#2ca02c'
                                                    }),
                                            html.
                                            H2(id='est-stockout-date',
                                               style={'textAlign': 'center'},
                                               title=
                                               'Projected date of stock depletion.'
                                               )
                                        ],
                                        style={
                                            'border': '1px solid #ddd',
                                            'borderRadius': '5px',
                                            'padding': '20px',
                                            'width': '23%',
                                            'backgroundColor': '#f5fff5',
                                            'boxShadow':
                                            '2px 2px 5px rgba(0,0,0,0.1)',
                                            'cursor': 'pointer',
                                            'margin': '10px'
                                        }),

                                    # Daily Run Rate
                                    html.Div(
                                        [
                                            html.H4('Daily Run Rate',
                                                    style={
                                                        'textAlign': 'center',
                                                        'color': '#d62728'
                                                    }),
                                            html.
                                            H2(id='daily-run-rate',
                                               style={'textAlign': 'center'},
                                               title=
                                               'Avg number of units sold per day.'
                                               )
                                        ],
                                        style={
                                            'border': '1px solid #ddd',
                                            'borderRadius': '5px',
                                            'padding': '20px',
                                            'width': '23%',
                                            'backgroundColor': '#fff0f5',
                                            'boxShadow':
                                            '2px 2px 5px rgba(0,0,0,0.1)',
                                            'cursor': 'pointer',
                                            'margin': '10px'
                                        }),
                                ],
                                style={
                                    'display': 'flex',
                                    'justifyContent': 'space-around',
                                    'flexWrap': 'wrap'
                                },
                                id='stat-cards-container')
                        ],
                        id='collapsible-stat-cards'),

                    # Collapsible Inventory Status Table
                    html.Div([
                        html.Button("Toggle Inventory Status Table",
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
                                    }),
                        html.Div([
                            html.H2('Current Inventory Status',
                                    style={
                                        'color': '#003366',
                                        'textAlign': 'center'
                                    }),
                            html.Div(id='inventory-status-table',
                                     style={
                                         'border': '1px solid #ddd',
                                         'borderRadius': '5px',
                                         'padding': '10px',
                                         'backgroundColor': '#ffffff'
                                     }),
                        ],
                                 id='inventory-table-container',
                                 style={'display': 'block'})
                    ],
                             id='collapsible-inventory-table'),

                    # Collapsible Time Series Graphs
                    html.Div([
                        html.Button("Toggle Time Series Graphs",
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
                                    }),
                        html.Div([
                            html.H2(
                                'Actual Qty. Sold & Current Daily Run Rate',
                                style={
                                    'color': '#003366',
                                    'textAlign': 'center'
                                }),
                            dcc.Graph(id='qty-sold-time-series'),
                            html.H2('Inventory On Hand Trend',
                                    style={
                                        'color': '#003366',
                                        'textAlign': 'center',
                                        'marginTop': '50px'
                                    }),
                            dcc.Graph(id='inventory-on-hand-time-series'),
                        ],
                                 id='time-series-container',
                                 style={'display': 'block'})
                    ],
                             id='collapsible-time-series'),
                ],
                style={
                    'marginLeft': '22%',  # To accommodate the fixed sidebar
                    'padding': '20px'
                }),
        ])


###############################################################################
#                     CALLBACKS FOR THE "PRODUCT SUMMARY" TAB
###############################################################################
def register_product_summary_callbacks(app, inventory_run_rate_df_cached,
                                       merged_df_cached,
                                       inventory_details_df_cached):
    """
    Register all callbacks needed by the product_summary tab.
    """

    logger.info('Registering callbacks for the product_summary tab.')

    # Toggle Stat Cards
    @app.callback(Output("stat-cards-container", "style"),
                  Input("toggle-stat-cards", "n_clicks"),
                  State("stat-cards-container", "style"))
    def toggle_stat_cards(n_clicks, current_style):
        if current_style is None:
            current_style = {'display': 'flex'}
        if n_clicks % 2 == 1:
            return {**current_style, 'display': 'none'}
        else:
            return {**current_style, 'display': 'flex'}

    # Toggle Inventory Table
    @app.callback(Output("inventory-table-container", "style"),
                  Input("toggle-inventory-table", "n_clicks"),
                  State("inventory-table-container", "style"))
    def toggle_inventory_table(n_clicks, current_style):
        if current_style is None:
            current_style = {'display': 'block'}
        if n_clicks % 2 == 1:
            return {**current_style, 'display': 'none'}
        else:
            return {**current_style, 'display': 'block'}

    # Toggle Time Series
    @app.callback(Output("time-series-container", "style"),
                  Input("toggle-time-series", "n_clicks"),
                  State("time-series-container", "style"))
    def toggle_time_series(n_clicks, current_style):
        if current_style is None:
            current_style = {'display': 'block'}
        if n_clicks % 2 == 1:
            return {**current_style, 'display': 'none'}
        else:
            return {**current_style, 'display': 'block'}

    # Main callback to update all summary items
    @app.callback([
        Output('quantity-in-stock', 'children'),
        Output('est-stock-days-on-hand', 'children'),
        Output('est-stockout-date', 'children'),
        Output('daily-run-rate', 'children'),
        Output('inventory-status-table', 'children'),
        Output('qty-sold-time-series', 'figure'),
        Output('inventory-on-hand-time-series', 'figure')
    ], [Input('product-dropdown', 'value')])
    def update_dashboard(selected_product):
        if not selected_product:
            # Return placeholders
            return (
                "-", "-", "-", "-",
                html.Div([
                    'Please select a product to view its inventory status and sales data.'
                ]), go.Figure(), go.Figure())

        # Filter for the selected product
        df_run_rate = inventory_run_rate_df_cached[
            inventory_run_rate_df_cached['name'] == selected_product]

        if df_run_rate.empty:
            return ("-", "-", "-", "-",
                    html.Div(['No inventory data available for this product.'
                              ]), go.Figure(), go.Figure())

        # Convert types as needed
        df_run_rate['restock_point'] = df_run_rate['restock_point'].astype(
            float).round().astype('int64')
        df_run_rate['est_stock_days_on_hand'] = df_run_rate[
            'est_stock_days_on_hand'].astype(float).round().astype('int64')
        df_run_rate['run_rate'] = df_run_rate['run_rate'].astype(
            float).round().astype('int64')

        df_run_rate = df_run_rate.astype({
            'inventory_id':
            'int64',
            'total_fulfillable_quantity':
            'int64',
            'est_stock_days_on_hand':
            'int64',
            'estimated_stockout_date':
            'datetime64[ns]',
            'restock_point':
            'int64',
            'actual_qty_sold_last_30_days':
            'int64',
            'actual_qty_sold_last_60_days':
            'int64',
            'actual_qty_sold_last_90_days':
            'int64'
        })

        # Extract metrics
        quantity_in_stock = df_run_rate['total_fulfillable_quantity'].iloc[0]
        est_stock_days = df_run_rate['est_stock_days_on_hand'].iloc[0]
        est_stockout_date = df_run_rate['estimated_stockout_date'].iloc[0]
        daily_run_rate = df_run_rate['run_rate'].iloc[0]

        if pd.notnull(est_stockout_date):
            est_stockout_date_formatted = est_stockout_date.strftime(
                '%Y-%m-%d')
        else:
            est_stockout_date_formatted = "N/A"

        # Reorder columns
        columns_order = [
            'inventory_id', 'name', 'run_rate', 'total_fulfillable_quantity',
            'restock_point', 'est_stock_days_on_hand',
            'estimated_stockout_date', 'actual_qty_sold_last_30_days',
            'actual_qty_sold_last_60_days', 'actual_qty_sold_last_90_days'
        ]
        df_run_rate = df_run_rate[columns_order]

        # DataTable
        inventory_table = dash_table.DataTable(
            data=df_run_rate.to_dict('records'),
            columns=[
                {
                    'name': 'Shipbob Inventory ID',
                    'id': 'inventory_id'
                },
                {
                    'name': 'Name',
                    'id': 'name'
                },
                {
                    'name': 'Daily Run Rate',
                    'id': 'run_rate'
                },
                {
                    'name': 'Total Fulfillable Quantity',
                    'id': 'total_fulfillable_quantity'
                },
                {
                    'name': 'Restock Point',
                    'id': 'restock_point'
                },
                {
                    'name': 'Est. Stock Days On Hand',
                    'id': 'est_stock_days_on_hand'
                },
                {
                    'name': 'Est. Stockout Date',
                    'id': 'estimated_stockout_date'
                },
                {
                    'name': 'Actual Qty. Sold - Past 30 Days',
                    'id': 'actual_qty_sold_last_30_days'
                },
                {
                    'name': 'Actual Qty. Sold - Past 60 Days',
                    'id': 'actual_qty_sold_last_60_days'
                },
                {
                    'name': 'Actual Qty. Sold - Past 90 Days',
                    'id': 'actual_qty_sold_last_90_days'
                },
            ],
            style_table={'overflowX': 'auto'},
            style_cell={
                'minWidth': '150px',
                'width': '180px',
                'maxWidth': '200px',
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
            style_data_conditional=[{
                'if': {
                    'row_index': 'odd'
                },
                'backgroundColor': '#f2f2f2'
            }],
            page_size=10,
            sort_action='native',
            filter_action='native',
            row_selectable='multi',
            tooltip_delay=0,
            tooltip_duration=None,
        )

        # Time series for sales
        df_merged = merged_df_cached[merged_df_cached['name'] ==
                                     selected_product]
        df_inventory_details = inventory_details_df_cached[
            inventory_details_df_cached['name'] == selected_product]

        try:
            restock_point = int(df_run_rate['restock_point'].iloc[0])
        except:
            restock_point = 0

        try:
            daily_run_rate_val = int(df_run_rate['run_rate'].iloc[0])
        except:
            daily_run_rate_val = 0

        # ===== QTY SOLD & DAILY RUN RATE FIGURE =====
        if df_merged.empty:
            fig1 = go.Figure()
            fig1.add_annotation(
                text="No sales data available for the selected product.",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=20))
        else:
            fig1 = go.Figure()

            # Actual quantity sold
            fig1.add_trace(
                go.Scatter(x=df_merged['created_date'],
                           y=df_merged['inventory_qty'],
                           mode='lines+markers',
                           name='Actual Quantity Sold',
                           line=dict(color='#1f77b4'),
                           marker=dict(color='#1f77b4')))

            # Daily Run Rate as a horizontal line
            fig1.add_trace(
                go.Scatter(
                    x=[
                        df_merged['created_date'].min(),
                        df_merged['created_date'].max()
                    ],
                    y=[daily_run_rate_val, daily_run_rate_val],
                    mode='lines',
                    name='Daily Run Rate',
                    line=dict(color='#ff7f0e', dash='dash'),
                ))

            fig1.update_layout(
                title='Actual Qty. Sold & Current Daily Run Rate',
                xaxis_title='Date',
                yaxis_title='Quantity',
                legend_title='Legend',
                xaxis=dict(rangeselector=dict(buttons=[
                    dict(count=7, label='1w', step='day', stepmode='backward'),
                    dict(
                        count=1, label='1m', step='month',
                        stepmode='backward'),
                    dict(
                        count=3, label='3m', step='month',
                        stepmode='backward'),
                    dict(step='all')
                ]),
                           rangeslider=dict(visible=True),
                           type='date'),
                plot_bgcolor='#f9f9f9',
                paper_bgcolor='#ffffff',
                font=dict(color='#333333'),
                margin=dict(l=50, r=150, t=50, b=50))

        # ===== INVENTORY ON HAND & RESTOCK POINT FIGURE =====
        if df_inventory_details.empty:
            fig2 = go.Figure()
            fig2.add_annotation(
                text="No inventory data available for the selected product.",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=20))
        else:
            fig2 = go.Figure()

            df_inventory_details = df_inventory_details.sort_values(
                'partition_date')

            # Inventory on hand line
            fig2.add_trace(
                go.Scatter(
                    x=df_inventory_details['partition_date'],
                    y=df_inventory_details['total_fulfillable_quantity'],
                    mode='lines+markers',
                    name='Inventory On Hand',
                    line=dict(color='#2ca02c'),
                    marker=dict(color='#2ca02c')))

            # Restock point horizontal line
            fig2.add_trace(
                go.Scatter(x=[
                    df_inventory_details['partition_date'].min(),
                    df_inventory_details['partition_date'].max()
                ],
                           y=[restock_point, restock_point],
                           mode='lines',
                           name='Restock Point',
                           line=dict(color='green', dash='dot')))

            # Estimated Stockout Date vertical line
            est_stockout_date_val = df_run_rate[
                'estimated_stockout_date'].iloc[0]
            if pd.notnull(est_stockout_date_val):
                est_stockout_date_val = pd.to_datetime(est_stockout_date_val)
                ymin = df_inventory_details['total_fulfillable_quantity'].min(
                ) * 0.95
                ymax = df_inventory_details['total_fulfillable_quantity'].max(
                ) * 1.05
                fig2.add_trace(
                    go.Scatter(
                        x=[est_stockout_date_val, est_stockout_date_val],
                        y=[ymin, ymax],
                        mode='lines',
                        name='Estimated Stockout Date',
                        line=dict(color='red', dash='dot'),
                    ))

            fig2.update_layout(
                title='Inventory On Hand Trend',
                xaxis_title='Date',
                yaxis_title='Quantity',
                legend_title='Legend',
                xaxis=dict(rangeselector=dict(buttons=[
                    dict(count=7, label='1w', step='day', stepmode='backward'),
                    dict(
                        count=1, label='1m', step='month',
                        stepmode='backward'),
                    dict(
                        count=3, label='3m', step='month',
                        stepmode='backward'),
                    dict(step='all')
                ]),
                           rangeslider=dict(visible=True),
                           type='date'),
                plot_bgcolor='#f9f9f9',
                paper_bgcolor='#ffffff',
                font=dict(color='#333333'),
                margin=dict(l=50, r=150, t=50, b=50))

        return (quantity_in_stock, est_stock_days, est_stockout_date_formatted,
                daily_run_rate, inventory_table, fig1, fig2)

    # If a product card was clicked, we set 'selected-product-store' to that product
    # Then we update the dropdown below:
    @app.callback(
        Output('product-dropdown', 'value'),
        Input('selected-product-store', 'data')
    )
    def update_product_dropdown(selected_product):
        logger.info("DEBUG: selected_product:", selected_product)
        logger.info("DEBUG: product_options list:",
              [o['value'] for o in product_options])

        # 1) If product_options is empty (edge case), don't crash
        if not product_options:
            return dash.no_update

        # 2) If we have no selected product, default to the first item
        if not selected_product:
            forced_value = product_options[0]['value']
            logger.info("DEBUG: forcing dropdown value to:", forced_value)
            return forced_value

        # 3) If a card was clicked, use that product
        return selected_product

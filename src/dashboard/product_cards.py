from dash import dcc, html, Input, Output, State, ALL, dash_table, callback, ctx
import dash
import pandas as pd
from loguru import logger


###############################################################################
#                   HELPER FUNCTIONS / ANY UTILITIES FOR THIS TAB
###############################################################################
def get_color(value):
    """
    Color logic for Est. Stock Days On Hand.
    """
    if value < 60:
        return 'darkred'
    elif 60 <= value < 70:
        return 'red'
    elif 70 <= value <= 100:
        return 'orange'
    else:  # value > 100
        return 'green'


###############################################################################
#             LAYOUT FOR THE "PRODUCT CARDS" TAB (RETURN dcc.Tab)
###############################################################################
def get_product_cards_tab(est_stock_days_on_hand_min,
                          est_stock_days_on_hand_max):
    """
    Returns the dcc.Tab layout for the 'Product Cards' page.
    """
    return dcc.Tab(
        label='Product Cards',
        value='product-cards',
        children=[
            html.Div(
                [
                    html.H1('Estimated Stock Days On Hand by Product',
                            style={
                                'textAlign': 'center',
                                'color': '#003366',
                                'marginTop': '20px'
                            }),

                    # Color Legend
                    html.Div([
                        html.H4('Color Legend:',
                                style={
                                    'textAlign': 'center',
                                    'color': '#003366',
                                    'marginTop': '20px'
                                }),
                        html.Ul([
                            html.Li([
                                html.Span(
                                    style={
                                        'backgroundColor': 'darkred',
                                        'display': 'inline-block',
                                        'width': '20px',
                                        'height': '20px',
                                        'marginRight': '10px'
                                    }),
                                "< 60 days of stock on hand. An MO for this product should already exist..."
                            ],
                                    style={'marginBottom': '10px'}),
                            html.Li([
                                html.Span(
                                    style={
                                        'backgroundColor': 'red',
                                        'display': 'inline-block',
                                        'width': '20px',
                                        'height': '20px',
                                        'marginRight': '10px'
                                    }),
                                "60 - 70 days of stock on hand. An MO for this product should already exist..."
                            ],
                                    style={'marginBottom': '10px'}),
                            html.Li([
                                html.Span(
                                    style={
                                        'backgroundColor': 'orange',
                                        'display': 'inline-block',
                                        'width': '20px',
                                        'height': '20px',
                                        'marginRight': '10px'
                                    }),
                                "70 - 100 days of stock on hand. Approaching the need to submit a MO..."
                            ],
                                    style={'marginBottom': '10px'}),
                            html.Li([
                                html.Span(
                                    style={
                                        'backgroundColor': 'green',
                                        'display': 'inline-block',
                                        'width': '20px',
                                        'height': '20px',
                                        'marginRight': '10px'
                                    }), "> 100 days of stock on hand..."
                            ])
                        ],
                                style={'listStyleType': 'none'})
                    ],
                             style={'margin': '20px'}),
                    html.Button("Reset Hidden Cards",
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
                                }),
                    # Slider
                    html.Div([
                        html.Label('Select Est. Stock Days On Hand Range:',
                                   style={'fontWeight': 'bold'}),
                        dcc.RangeSlider(
                            id='est-stock-days-slider',
                            min=est_stock_days_on_hand_min,
                            max=est_stock_days_on_hand_max,
                            value=[
                                est_stock_days_on_hand_min,
                                est_stock_days_on_hand_max
                            ],
                            marks={
                                i: str(i)
                                for i in range(est_stock_days_on_hand_min,
                                               est_stock_days_on_hand_max +
                                               1, 10)
                            },
                            step=1,
                            allowCross=False)
                    ],
                             style={'margin': '20px'}),
                    html.Div(id='kpi-cards-container',
                             style={
                                 'display': 'flex',
                                 'flexWrap': 'wrap',
                                 'justifyContent': 'center',
                                 'marginTop': '20px'
                             })
                ],
                style={'padding': '20px'})
        ])


###############################################################################
#                     CALLBACKS FOR THE "PRODUCT CARDS" TAB
###############################################################################
def register_product_cards_callbacks(app, inventory_run_rate_df_cached):
    """
    Register all callbacks needed by the product_cards tab.
    """

    # Update KPI cards
    @app.callback(
        Output('kpi-cards-container', 'children'),
        [
            Input('tabs', 'value'),
            Input('hidden-cards-store', 'data'),
            Input('est-stock-days-slider', 'value')
        ],
    )
    def update_kpi_cards(selected_tab, hidden_cards, est_stock_days_range):
        if selected_tab != 'product-cards':
            return []

        df = inventory_run_rate_df_cached[['name',
                                           'est_stock_days_on_hand']].copy()

        # Filter by slider
        min_range, max_range = est_stock_days_range
        df = df[(df['est_stock_days_on_hand'] >= min_range)
                & (df['est_stock_days_on_hand'] <= max_range)]

        # Sort ascending
        df = df.sort_values(by='est_stock_days_on_hand', ascending=True)

        # Remove hidden cards
        if hidden_cards:
            df = df[~df['name'].isin(hidden_cards)]

        # Build the cards
        cards = []
        for _, row in df.iterrows():
            name = row['name']
            value = row['est_stock_days_on_hand']
            color = get_color(value)

            card = html.Div(
                [
                    # Hide button
                    html.Button('X',
                                id={
                                    'type': 'hide-button',
                                    'index': name
                                },
                                n_clicks=0,
                                style={
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
                        html.H4(name,
                                style={
                                    'textAlign': 'center',
                                    'color': '#ffffff',
                                    'fontSize': '16px',
                                    'marginTop': '20px'
                                }),
                        html.H2(f"{value}",
                                style={
                                    'textAlign': 'center',
                                    'color': '#ffffff',
                                    'fontSize': '24px'
                                })
                    ],
                             style={'cursor': 'pointer'},
                             id={
                                 'type': 'product-card',
                                 'index': name
                             },
                             n_clicks=0)
                ],
                style={
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

    # Hide cards
    @app.callback(Output('hidden-cards-store', 'data'), [
        Input({
            'type': 'hide-button',
            'index': ALL
        }, 'n_clicks'),
        Input('reset-hidden-cards', 'n_clicks')
    ], [
        State({
            'type': 'hide-button',
            'index': ALL
        }, 'id'),
        State('hidden-cards-store', 'data')
    ])
    def hide_card(n_clicks_list, reset_n_clicks, ids, hidden_cards):
        if hidden_cards is None:
            hidden_cards = []

        # Figure out which triggered
        if ctx.triggered and 'reset-hidden-cards' in ctx.triggered[0][
                'prop_id']:
            # The reset button was clicked
            return []

        # For each button, if it was clicked, add that product to hidden
        for n_clicks, id_dict in zip(n_clicks_list, ids):
            if n_clicks and (id_dict['index'] not in hidden_cards):
                hidden_cards.append(id_dict['index'])

        return hidden_cards

    # Handle card clicks -> switch to product-summary tab
    @app.callback(
        [Output('tabs', 'value'),
         Output('selected-product-store', 'data')],
        [Input({
            'type': 'product-card',
            'index': ALL
        }, 'n_clicks')], [
            State({
                'type': 'product-card',
                'index': ALL
            }, 'id'),
            State({
                'type': 'product-card',
                'index': ALL
            }, 'n_clicks_timestamp')
        ])
    def on_card_click(n_clicks_list, ids, n_clicks_timestamps):
        if not n_clicks_timestamps:
            return dash.no_update, dash.no_update

        # Find the card that was clicked last (max timestamp)
        valid_timestamps = [ts for ts in n_clicks_timestamps if ts is not None]
        if not valid_timestamps:
            return dash.no_update, dash.no_update

        max_timestamp = max(valid_timestamps)
        max_index = n_clicks_timestamps.index(max_timestamp)
        card_id = ids[max_index]['index']  # This is the product name

        # Switch to 'product-summary' tab
        return 'product-summary', card_id

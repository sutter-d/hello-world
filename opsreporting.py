#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 12:00:38 2022

@author: danielsutter
"""

import pandas as pd
import pathlib
import datetime as dt
import dash
from dash.dependencies import Input, Output
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import ds_utils as ds
# from dash_google_oauth.google_auth import GoogleAuth
# from dotenv import load_dotenv
#
# # Declare app
#
# # Trying to initiate oauth
# load_dotenv()  # take environment variables from .env.
# app = dash.Dash(__name__)
# app.config.external_stylesheets = [dbc.themes.ZEPHYR]
# auth = GoogleAuth(app)

# Without oauth
app = dash.Dash()
app.config.external_stylesheets = [dbc.themes.ZEPHYR]

# NAVBAR CONTAINER
# The nav bar sits at the top of
# the page and acts like a banner for each page
navbar = dbc.Navbar(
    dbc.Container(
        dbc.Row(
            [
                dbc.Col(
                    dbc.CardImg(
                        src=app.get_asset_url('rackrender.jpg')
                    ),
                    width={'size': 2, 'offset': 1},
                ),
                dbc.Col(
                    dbc.CardImg(
                        src=app.get_asset_url('ox.jpg')
                    ),
                    width={'size': 2, 'offset': 6},
                )
            ],
            align='center',
            # justify='around'
        )
    ),
    color="rgb(23, 59, 48)"  # matching Oxide symbol
)

# The jumbo is the main container
# on the page for content
jumbo_pic_six = dbc.Col(
    dbc.Container(
        html.Img(
            src=app.get_asset_url('rackv2.jpg'),
            # className="mx-auto",
            style={"width": "6",
                   "display": "block"}
        ),
    )
)
jumbo_pic_four = dbc.Col(
    dbc.Container(
        html.Img(
            src=app.get_asset_url('rackv2.jpg'),
            # className="mx-auto",
            style={"width": "4",
                   "display": "block"}
        ),
    )
)
jumbo_text = dbc.Col(
    dbc.Container(
        [
            # html.H2("Oxide Ops Reporting",
            #         className="text-end text-light display-3"),
            html.H2("Oxide",
                    className="text-end text-light display-3"),
            html.H2("Ops",
                    className="text-end text-light display-3"),
            html.H2("Reporting",
                    className="text-end text-light display-3"),

            html.Hr(className="my-2"),
            html.P("Dev Environment", className="text-end")
        ],
        className="p-5 text-light",
        # style={"width": "2",
        # "display": "block"}
    )
)
jumbo = dbc.Container(
    dbc.Row(
        [
            jumbo_text,
            jumbo_pic_six
        ],
        align='center',
        justify='between'
    ),
)

# Create a body container here to pull
# the nav bar and jumbo together
body = dbc.Col(
    # dbc.Container(
    children=[
        navbar,
        jumbo,
    ],
    # ),
    id='page-content',
)

# the styles for the main content position
# it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "12rem",
    # "margin-right": "0", # WANT THIS FLUSH TO RIGHT MARGIN
    # "padding": "0.5rem 0rem",
    # "background-color": "#0a1111",  # COLOR MATCHED TO RACK SCREENSHOT
    "height": "100vh",
    "fluid": "True"
}

content = html.Div(body, style=CONTENT_STYLE)

# the style arguments for the sidebar. We use position:fixed and a fixed width
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "12rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# SIDEBAR CUSTOMIZED TO OPS CONTENT
sidebar = html.Div(
    [
        html.H2("Ops @ Oxide"),
        html.Hr(),
        html.P(
            "All our reports in One Place", className="lead"
        ),
        dbc.Nav(
            [
                dbc.NavLink("HOME", href="/", active="exact"),
                dbc.NavLink("LOT PLAN", href="/page-1", active="exact"),
                dbc.NavLink("KPIs", href="/page-2", active="exact"),
                dbc.NavLink("CTB", href="/page-3", active="exact"),
                dbc.NavLink("MRP", href="/page-4", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

# This is the main page layout container
# The two components are the sidebar and content containers
app.layout = html.Div(
    [
        dcc.Location(id="url"),
        sidebar,
        content
    ]
)

# Lot Plan tab_content
# The lot plan tab has both the
# lot plan and master production
# schedule pulled from gdrive
lot_gdrive = './data/mps.xlsx'
lot_data = pd.read_excel(lot_gdrive,
                         sheet_name='Lot Plan',
                         header=0)

lot_data = lot_data.iloc[8:15, 1:8]
lot_data = lot_data.reset_index(drop=True).fillna(0)
lot_data = lot_data.drop(lot_data.columns[2], axis=1)
lot_data = lot_data.to_numpy()
cols = [
    "cpn",
    "description",
    "2023.Lot1",
    "2023.Lot2",
    "2023.Lot3",
    "2023.Lot4"
]

lot_pd = pd.DataFrame(data=lot_data, columns=cols)

lot_table = dash_table.DataTable(
    data=lot_pd.to_dict('records'),
    columns=[{'id': c, 'name': c} for c in lot_pd.columns],
    page_size=10,
    filter_action='native',
    sort_action='native',
    style_table={
        'overflowX': 'auto',
        "paddingTop": 10,
        "paddingBottom": 10,
        "paddingLeft": 10,
        "paddingRight": 10,
    },
    style_cell={
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 180,
        # 'overflowX': 'auto'
    },
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white',
        'text_align': 'left'
    },
    tooltip_data=[
        {
            column: {'value': str(value), 'type': 'markdown'}
            for column, value in row.items()
        } for row in lot_pd.to_dict('records')
    ],
    tooltip_duration=None,
    id="lot_table"
)

mps_gdrive = './data/mps.xlsx'
mps_data = pd.read_excel(mps_gdrive,
                         sheet_name='Master Schedule',
                         header=4)

mps_data = ds.clean_mps(mps_data)
iso_dt = dt.date.isocalendar(dt.datetime.now())
mps_data = mps_data[mps_data['year'] >= iso_dt[0]]
mps_data = mps_data[mps_data['wk'] >= iso_dt[1]]

mps_table = dash_table.DataTable(
    data=mps_data.to_dict('records'),
    columns=[{'id': c, 'name': c} for c in mps_data.columns],
    page_size=30,
    filter_action='native',
    sort_action='native',
    style_table={
        'overflowX': 'auto',
        'minWidth': '100%',
        "paddingTop": 10,
        "paddingBottom": 10,
        "paddingLeft": 10,
        "paddingRight": 10,
    },
    style_cell={
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 180,
        # 'overflowX': 'auto'
    },
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white',
        'text_align': 'left'
    },
    tooltip_data=[
        {
            column: {'value': str(value), 'type': 'markdown'}
            for column, value in row.items()
        } for row in mps_data.to_dict('records')
    ],
    tooltip_duration=None,
    id="mps-table"
)

lot_text = dbc.Container(
    dbc.Col(
        html.Div("Lot Plan", className="text-light p-3 display-6"),
        width={"size": 6, "offset": 0},
    )
)

mps_text = dbc.Container(
    dbc.Col(
        html.Div("Master Schedule", className="text-light p-3 display-6"),
        width={"size": 6, "offset": 0},
    )
)

lot_content = [
    navbar,
    lot_text,
    dbc.Container(lot_table),
    mps_text,
    dbc.Container(dbc.Row([dbc.Col(mps_table), jumbo_pic_six], align="center", justify="around"))
]

# KPIs tab content
# The KPI tab tracks kpis over time and
# plots red yellow and ok on 4 graphs by LOT
inv_gdrive = './data/old_ctb.xlsx'
kpi_data = pd.read_excel(inv_gdrive,
                         sheet_name='KPIs',
                         header=0)

kpi_table = dash_table.DataTable(
    # data=tbl_data.to_dict('records'),
    columns=[{'id': c, 'name': c} for c in kpi_data.columns],
    filter_action='native',
    sort_action='native',
    tooltip_data=[
        {
            column: {'value': str(value), 'type': 'markdown'}
            for column, value in row.items()
        } for row in kpi_data.to_dict('records')
    ],
    style_table={
        'overflowX': 'auto',
        "paddingTop": 30,
        "paddingBottom": 10,
        "paddingLeft": 10,
        "paddingRight": 10,
    },
    style_cell={
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 180,
        # 'overflowX': 'auto'
    },
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white',
        'text_align': 'left'
    },
    tooltip_duration=None,
    id="kpi-data-id",
    # id="kpi_table"
)

# The checklist defaults to red yellow and ok selected
kpi_checklist = dcc.Checklist(
    id="kpi_checklist",
    options=["1-Red", "2-Yellow", "3-Ok"],
    value=["1-Red", "2-Yellow"],
    inline=True,
    style={"padding": "0rem 2rem"}
)

# Each page will have a content container
# with the nav bar and all the special
# containers for that page
kpi_content = [
    navbar,
    kpi_graph := dcc.Graph(),
    kpi_checklist,
    kpi_table
]

# CTB tab_content
# The ctb tab polls the latest component
# forecast from gdive drive and displays
# it with sorting and filtering
ctb_data = pd.read_excel(inv_gdrive,
                         # sheet_name='Clear.To.Build',
                         sheet_name='Full.Comp.Forecast',
                         header=0)

ctb_table = dash_table.DataTable(
    data=ctb_data.to_dict('records'),
    columns=[{'id': c, 'name': c} for c in ctb_data.columns],
    page_size=30,
    filter_action='native',
    sort_action='native',
    style_table={
        'overflowX': 'auto',
        "paddingTop": 10,
        "paddingBottom": 10,
        "paddingLeft": 10,
        "paddingRight": 10,
    },
    style_cell={
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 180,
    },
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data_conditional=[
        {
            'if': {
                'filter_query': '{lot1_ok} = 1-Red',
                'column_id': 'lot1_ok'
            },
            'color': 'tomato',
            'fontWeight': 'bold',
        },
        {
            'if': {
                'filter_query': '{lot2_ok} = 1-Red',
                'column_id': 'lot2_ok'
            },
            'color': 'tomato',
            'fontWeight': 'bold',
        },
        {
            'if': {
                'filter_query': '{lot3_ok} = 1-Red',
                'column_id': 'lot3_ok'
            },
            'color': 'tomato',
            'fontWeight': 'bold',
        },
        {
            'if': {
                'filter_query': '{lot4_ok} = 1-Red',
                'column_id': 'lot4_ok'
            },
            'color': 'tomato',
            'fontWeight': 'bold',
        },
    ],
    tooltip_data=[
        {
            column: {'value': str(value), 'type': 'markdown'}
            for column, value in row.items()
        } for row in ctb_data.to_dict('records')
    ],
    tooltip_duration=None,
    id="ctb_table",
    export_format="xlsx"
)

ctb_mkdn = dcc.Markdown('''
    This is the complete component forecast with the following **CTB Filters**. [Link to Clear to Build analysis](https://drive.google.com/drive/folders/1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON)
    ```python
    if opn orders + on hand inventory < forecast:
        return '1-Red'
    ```

    ```python
    if lot_ok == '1-Red':
        return '1-Red'
    elif on hand inventory < forecast:
        return '2-Yellow'
    else:
        return '3-Ok'
    ```
    The report is sorted Red > Yellow > Ok for lot4_ok
    ''',
                        className="text-light px-4"
                        )

ctb_text = dbc.Container(
    dbc.Row(
        [
            dbc.Container(
                dbc.Col(
                    html.Div("CTB Report", className="text-light p-3 display-6"),
                    width={"size": 6, "offset": 0},
                )
            ),
            ctb_mkdn
        ],
    )
)

ctb_content = [
    navbar,
    ctb_text,
    ctb_table,
]

# MRP tab_content
# PROCUREMENT TRACKER FROM GDRIVE ETL
inv_gdrive = './data/ox_prod_inv.xlsx'
open_po_get = pd.read_excel(inv_gdrive,
                            sheet_name=0,  # switching to position instead of sheet name
                            header=0)

on_hand_get = pd.read_excel(inv_gdrive,

                            sheet_name=1,
                            header=0)

inv = ds.oxinv(open_po_get, on_hand_get)

# MRP data stored in gdrive ETL
mrp_gdrive = './data/mrp_export.xlsx'
mrp_data = pd.read_excel(mrp_gdrive,
                         sheet_name='mrp_raw',
                         header=0)
mrp_data = mrp_data.merge(inv, 'left', 'cpn').fillna(0)
mrp_data = mrp_data.sort_values(by=['cpn', 'nbd_year', 'need_by_date'])
mrp_data['order_cum'] = mrp_data.groupby(by=['cpn'])['ord_qty'].cumsum()
mrp_data['inv_roll'] = mrp_data.apply(lambda x: x['on_hand'] - x['order_cum'], axis=1)

# Creating an On Hand entry line item
on_hand = mrp_data.iloc[0:1, :]  # .reset_index(drop=True)
# on_hand = on_hand.append(inv)
on_hand = on_hand.append(mrp_data[['cpn', 'on_hand']].groupby(by=['cpn'], as_index=False).first())
on_hand['name'] = 'On Hand Qty'
on_hand['total_qty'] = on_hand['on_hand']
on_hand['inv_roll'] = on_hand['on_hand']

# on_hand = on_hand.groupby(by=['cpn'], as_index=False).first()

float_cols = on_hand.select_dtypes(include=['float64']).columns
str_cols = on_hand.select_dtypes(include=['object']).columns

on_hand.loc[:, float_cols] = on_hand.loc[:, float_cols].fillna(0)
on_hand.loc[:, str_cols] = on_hand.loc[:, str_cols].fillna('')
on_hand = on_hand.reset_index(drop=True)
on_hand = on_hand.iloc[1:, :].reset_index(drop=True)
mrp_data = on_hand.append(mrp_data)

# Creating an Open Order entry line item
open_order = mrp_data.iloc[0:1, :]  # .reset_index(drop=True)
# open_order = open_order.append(inv)
open_order = open_order.append(mrp_data[['cpn', 'open_orders']].groupby(by=['cpn'], as_index=False).first())
open_order['name'] = 'Open Order Qty'
open_order['total_qty'] = open_order['open_orders']
open_order['inv_roll'] = open_order['open_orders']

# open_order = open_order.groupby(by=['cpn'], as_index=False).first()

float_cols = open_order.select_dtypes(include=['float64']).columns
str_cols = open_order.select_dtypes(include=['object']).columns

open_order.loc[:, float_cols] = open_order.loc[:, float_cols].fillna(9999)
open_order.loc[:, str_cols] = open_order.loc[:, str_cols].fillna('')
open_order = open_order.reset_index(drop=True)
open_order = open_order.iloc[1:, :].reset_index(drop=True)
mrp_data = mrp_data.append(open_order)

cols_to_move = [
    "idx",
    "query_pn",
    "parent",
    "cpn",
    "name",
    "level",
    "lt",
    "total_qty",
    "ord_qty",
    "inv_roll",
    "order",
    "due_date",
    "due_year",
    "need_by_date",
    "nbd_year"
]

cols = cols_to_move + [col for col in mrp_data.columns if col not in cols_to_move]
mrp_data = mrp_data[cols]

cpn_filter = dcc.Dropdown(
    mrp_data['cpn'].sort_values(ascending=False).unique(),
    placeholder='CPN'
)
name_filter = dcc.Dropdown(
    mrp_data['name'].sort_values(ascending=False).unique(),
    placeholder='Name'
)
order_filter = dcc.Dropdown(
    mrp_data['order'].sort_values(ascending=True).unique(),
    placeholder='Order'
)
flat_filter = dcc.Dropdown(
    ['Show Next Level',
     'Show ALL Levels'],
    value='Show Next Level'
)

mrp_filter = dbc.Container(
    dbc.Row(
        [
            dbc.Col(cpn_filter),
            dbc.Col(name_filter),
            dbc.Col(order_filter)
        ],
        className="mb-3"
    )
)

parent_mkdn = dcc.Markdown(
    '''
    This **MRP** tab is a tool for reviewing parent-component relationships and inventory totals.
    
    [Link to Lead Time table](https://docs.google.com/spreadsheets/d/1D0PcawQYmnFd5F8BcNRK9fILWji78iOD/edit?usp=share_link&ouid=115825130364402950363&rtpof=true&sd=true)
    [Link to MRP file and Inventory file folder](https://drive.google.com/drive/folders/1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco)
    
    ''',
    className="text-light px-4"
)

parent_text = dbc.Container(
    dbc.Row(
        [
            dbc.Container(
                dbc.Col(
                    html.Div("Parent CPN", className="text-light p-3 display-6"),
                    width={"size": 6, "offset": 0},
                ),

            ),
            parent_mkdn

        ]
    )
)

component_text = dbc.Container(
    dbc.Row(
        [
            dbc.Col(
                html.Div("Component CPNs", className="text-light p-3 display-6"),
                width={"size": 5, "offset": 0},
            ),
            dbc.Col(
                flat_filter,
                width={"size": 3, "offset": 0}
            )

        ],
        align='center',
        justify='start'
    ),
)

parent_table = dash_table.DataTable(
    id='parent-data-id',
    columns=[{'id': c, 'name': c} for c in mrp_data.columns],
    page_size=30,
    sort_action='native',
    style_table={
        'overflowX': 'auto',
        "paddingTop": 10,
        "paddingBottom": 10,
        "paddingLeft": 10,
        "paddingRight": 10,
    },
    style_cell={
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 180,
    },
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data_conditional=[
        {
            'if': {
                'filter_query': '{inv_roll} <= 0',
                'column_id': 'inv_roll'
            },
            'color': 'tomato',
            'fontWeight': 'bold',
        }
    ],
    # tooltip_data='par-tool-tip-data',
    tooltip_duration=None,
    export_format="xlsx",
)

component_table = dash_table.DataTable(
    columns=[{'id': c, 'name': c} for c in mrp_data.columns],
    page_size=30,
    filter_action='native',
    sort_action='native',
    style_table={
        'overflowX': 'auto',
        "paddingTop": 10,
        "paddingBottom": 10,
        "paddingLeft": 10,
        "paddingRight": 10,
    },
    style_cell={
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 180,
        # 'overflowX': 'auto'
    },
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white',
        'text_align': 'left'
    },
    style_data_conditional=[
        {
            'if': {
                'filter_query': '{inv_roll} <= 0',
                'column_id': 'inv_roll'
            },
            'color': 'tomato',
            'fontWeight': 'bold',
        }
    ],
    # tooltip_data= 'comp-tool-tip-data',
    tooltip_duration=None,
    id="component-data-id",
    export_format="xlsx",
)

mrp_content = [
    navbar,
    parent_text,
    mrp_filter,
    dbc.Container(parent_table),
    component_text,
    dbc.Container(component_table)
]


@app.callback(Output(component_id="page-content", component_property="children"),
              Input(component_id="url", component_property="pathname"))
def render_page_content(pathname):
    if pathname == "/":
        return body

    elif pathname == "/page-1":
        return lot_content

    elif pathname == "/page-2":
        return kpi_content

    elif pathname == "/page-3":
        return ctb_content

    elif pathname == "/page-4":
        return mrp_content

    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )


# Available templates:
# ['ggplot2', 'seaborn', 'simple_white', 'plotly',
#  'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
#  'ygridoff', 'gridon', 'none']

@app.callback(Output(kpi_graph, component_property='figure'),
              Output('kpi-data-id', component_property='data'),
              Input(kpi_checklist, component_property="value"))
def update_kpi_line_chart(kpis):
    fig_data = kpi_data
    fig_data = pd.melt(kpi_data, id_vars=["assembly", 'kpi', 'date'], value_vars=['lot1', 'lot2', 'lot3', 'lot4'])
    fig_data = fig_data.sort_values(by=["assembly", 'kpi', 'variable', 'date'])
    mask = fig_data.kpi.isin(kpis)
    fig = px.line(fig_data[mask],
                  x="date",
                  y="value",
                  color='kpi',
                  facet_col="variable",
                  facet_row="assembly",
                  template='plotly_dark',
                  title='KPI Trends',
                  # text='value',
                  color_discrete_map={'1-Red': 'tomato', '2-Yellow': 'gold', '3-Ok': 'green'}
                  )
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(transition_duration=500)
    kpi_check_data = kpi_data[kpi_data['kpi'].isin(kpis)]
    return fig, kpi_check_data.to_dict('records')


@app.callback(Output('parent-data-id', component_property='data'),
              Output('component-data-id', component_property='data'),
              # Output('par-tool-tip-data', component_property='data'),
              # Output('comp-tool-tip-data', component_property='data'),
              Input(cpn_filter, component_property="value"),
              Input(name_filter, component_property="value"),
              Input(order_filter, component_property="value"),
              Input(flat_filter, component_property="value")
              )
def update_mrp_data_table(cpn_drop, name_drop, order_drop, flat_drop):
    parent_data = mrp_data
    component_data = mrp_data
    cpn = ''

    if cpn_drop is not None and order_drop is not None:
        parent_data = parent_data[parent_data['cpn'] == cpn_drop]
        parent_data = parent_data[parent_data['order'] == order_drop]
        if flat_drop == 'Show Next Level':
            component_data = component_data[component_data['parent'] == cpn_drop]
        else:
            component_data = component_data[component_data['pnladder'].str.contains(cpn_drop)]
        component_data = component_data[component_data['order'] == order_drop]

    if name_drop is not None and order_drop is not None:
        cpn = parent_data[parent_data['name'] == name_drop].reset_index(drop=True).at[0, 'cpn']
        parent_data = parent_data[parent_data['cpn'] == cpn]
        cpn = parent_data.reset_index(drop=True).at[0, 'cpn']
        if cpn != '':
            # component_data = component_data[component_data['parent'] == cpn]
            if flat_drop == 'Show Next Level':
                component_data = component_data[component_data['parent'] == cpn]
            else:
                component_data = component_data[component_data['pnladder'].str.contains(cpn)]
        parent_data = parent_data[parent_data['order'] == order_drop]
        component_data = component_data[component_data['order'] == order_drop]

    if cpn_drop is not None:
        parent_data = parent_data[parent_data['cpn'] == cpn_drop]
        if flat_drop == 'Show Next Level':
            component_data = component_data[component_data['parent'] == cpn_drop]
        else:
            component_data = component_data[component_data['pnladder'].str.contains(cpn_drop)]

    if name_drop is not None:
        cpn = parent_data[parent_data['name'] == name_drop].reset_index(drop=True).at[0, 'cpn']
        parent_data = parent_data[parent_data['cpn'] == cpn]
        cpn = parent_data.reset_index(drop=True).at[0, 'cpn']
        if cpn != '':
            # component_data = component_data[component_data['parent'] == cpn]
            if flat_drop == 'Show Next Level':
                component_data = component_data[component_data['parent'] == cpn]
            else:
                component_data = component_data[component_data['pnladder'].str.contains(cpn)]

    if order_drop is not None:
        parent_data = parent_data[parent_data['order'] == order_drop]
        component_data = component_data[component_data['order'] == order_drop]

    # par_tool_tip_data = [{column: {'value': str(value), 'type': 'markdown'} for column, value in row.items()} for row in parent_data.to_dict('records')]

    # comp_tool_tip_data = [{column: {'value': str(value), 'type': 'markdown'} for column, value in row.items()} for row in component_data.to_dict('records')]

    return parent_data.to_dict('records'), component_data.to_dict('records')  # , par_tool_tip_data, comp_tool_tip_data


if __name__ == '__main__':
    app.run_server(debug=True)
    # app.run_server(debug=False)

import functools
import json
import os
import sys
import time
import urllib.parse
from datetime import date

import dash_bootstrap_components as dbc
import duckdb
import plotly.express as px
from dash import Dash, dcc, html, Output, Input, State
from dotenv import load_dotenv

load_dotenv()

DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')

FILTER_NAMES = ['location_name', 'company_name', 'technology_name']

app = Dash('Dashy', title='Job Market Analytics', external_stylesheets=[dbc.themes.SANDSTONE])
server = app.server

time_selector = dbc.RadioItems(
    options=[
        {'label': 'Last Month', 'value': '1'},
        {'label': 'Last Quarter', 'value': '3'},
        {'label': 'Last Year', 'value': '12'},
    ],
    value='1',
    id='time-selector',
    inline=True,
)

controls = dbc.Card(
    [
        html.Div([
            html.H3("Time Range"),
            time_selector,
        ]),
        html.Br(),
        html.Div([
            html.H3('City'),
            dcc.Dropdown(
                options=[],
                id='location-selector',
                multi=True
            )
        ]),
        html.Br(),
        html.Div([
            html.H3('Company'),
            dcc.Dropdown(
                options=[],
                id='company-selector',
                multi=True
            )
        ]),
        html.Br(),
        html.Div([
            html.H3('Technology'),
            dcc.Dropdown(
                options=[],
                id='technology-selector',
                multi=True
            )
        ]),
    ],
    body=True,
)

app.layout = dbc.Container(
    [
        dcc.Location(id='url'),
        html.H1("Job Market Analytics"),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(controls, md=4),
                dbc.Col(html.Div(
                    [
                        html.Div(id='main-graph'),
                        html.Div(id='location-graph'),
                        html.Div(id='company-graph'),
                        html.Div(id='technology-graph'),
                    ]), md=8),
            ],
        ),
        html.Hr(),
        html.Div(
            id='performance-info'
        ),
    ],
    fluid=True,
)


def encode_param(value):
    encoded_value = json.dumps(value, separators=(',', ':')) if value else ''
    return encoded_value


def decode_params(url_hash, param_name):
    params = urllib.parse.parse_qs(url_hash.lstrip('#'))
    if param_name not in params.keys():
        return ''
    param = params[param_name]
    if isinstance(param, list) and len(param) > 0:
        param = param[0]
    param = json.loads(param)
    return param


@functools.lru_cache(maxsize=None)
def query_db(sql_statement, _=date.today()):
    conn = duckdb.connect(DUCKDB_DWH_FILE, read_only=True)
    df = conn.execute(sql_statement).df()
    conn.close()

    return df


@app.callback(
    Output('time-selector', 'value'),
    Output('location-selector', 'value'),
    Output('company-selector', 'value'),
    Output('technology-selector', 'value'),
    Input('url', 'pathname'),
    State('url', 'hash'),
    State('time-selector', 'value'),
)
def update_intial_values(_, url_hash, time_input):
    time_output = decode_params(url_hash, 'time') or time_input
    location_output = decode_params(url_hash, 'location')
    company_output = decode_params(url_hash, 'company')
    technology_output = decode_params(url_hash, 'technology')

    return [time_output, location_output, company_output, technology_output]


@app.callback(
    Output('url', 'hash'),
    Input('time-selector', 'value'),
    Input('location-selector', 'value'),
    Input('company-selector', 'value'),
    Input('technology-selector', 'value'),
)
def update_hash(time_input, location_input, company_input, technology_input):
    params = {
        'time': encode_param(time_input),
        'location': encode_param(location_input),
        'company': encode_param(company_input),
        'technology': encode_param(technology_input),
    }
    params = {k: v for k, v in params.items() if v}
    url_hash = urllib.parse.urlencode(params)
    return url_hash


@app.callback(
    Output('main-graph', 'children'),
    Output('location-graph', 'children'),
    Output('company-graph', 'children'),
    Output('technology-graph', 'children'),
    Output('performance-info', 'children'),
    Output('location-selector', 'options'),
    Output('company-selector', 'options'),
    Output('technology-selector', 'options'),
    Input('url', 'hash'),
)
def update_graphs(url_hash):
    start_time = time.time()

    inputs = {
        'time_name': decode_params(url_hash, 'time') or 1,
        'location_name': decode_params(url_hash, 'location'),
        'company_name': decode_params(url_hash, 'company'),
        'technology_name': decode_params(url_hash, 'technology'),
    }

    table_name = f'normalized_online_job_months_{inputs["time_name"]}'
    month_ending = 's' if int(inputs["time_name"]) > 1 else ''
    months_as_text = f'{inputs["time_name"]} month{month_ending}'

    where_clause = {}
    for filter_name in FILTER_NAMES:
        if inputs[filter_name]:
            where_clause[filter_name] = f'{filter_name} IN (SELECT UNNEST({inputs[filter_name]}  ))'

    main_where_clause = ' AND '.join(where_clause.values()) or '1 = 1'

    df = query_db(f'''
    WITH all_dates AS (
        SELECT DISTINCT online_at
          FROM {table_name}
         ORDER BY 1    
    ), total_jobs AS (
        SELECT online_at,
               COUNT(DISTINCT job_id) AS total_jobs
          FROM {table_name}
         WHERE {main_where_clause}
         GROUP BY 1
         ORDER BY 1    
    )
    SELECT ad.online_at,
           COALESCE(total_jobs, 0) as total_jobs
           FROM all_dates ad
           FULL OUTER JOIN total_jobs tj ON (ad.online_at = tj.online_at)
    ''')

    filter_df = {}
    compare_df = {}
    options = {}
    for filter_name in FILTER_NAMES:
        filter_where_clause_list = []
        for key, value in where_clause.items():
            if key != filter_name:
                filter_where_clause_list.append(value)
        filter_where_clause = ' AND '.join(filter_where_clause_list) or '1 = 1'
        filter_df[filter_name] = query_db(f'''
            SELECT {filter_name},
                   CAST(MAX(total_jobs) AS INTEGER) AS total_jobs
            FROM (
                SELECT {filter_name},
                       online_at,
                       COUNT(DISTINCT job_id) AS total_jobs
                  FROM {table_name}
                 WHERE {filter_where_clause}
                 GROUP BY 1, 2
            )
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10000
            ''')

        filter_df_records = filter_df[filter_name].to_dict('records')
        options[filter_name] = [
            {'label': f'{option[filter_name]} ({option["total_jobs"]})', 'value': option[filter_name]}
            for option in filter_df_records
        ]

        if 2 <= len(inputs[filter_name]) <= 20:
            compare_df[filter_name] = query_db(f'''
            WITH all_dates AS (
                SELECT DISTINCT online_at
                  FROM {table_name}
                 ORDER BY 1
            ), filter_options AS (
                SELECT DISTINCT {filter_name}
                  FROM {table_name}
                 WHERE {main_where_clause}
            ), cartesian_product AS (
                SELECT d.online_at,
                       fo.{filter_name}
                  FROM all_dates d
                 CROSS JOIN filter_options fo
            ), total_jobs AS (
                SELECT online_at,
                       {filter_name},
                       COUNT(DISTINCT job_id) AS total_jobs
                  FROM {table_name}
                 WHERE {main_where_clause}
                 GROUP BY 1, 2
            )
            SELECT cp.online_at,
                   cp.{filter_name},
                   COALESCE(tj.total_jobs, 0) AS total_jobs
              FROM cartesian_product cp
              FULL OUTER JOIN total_jobs tj ON (cp.online_at = tj.online_at AND cp.{filter_name} = tj.{filter_name})
             ORDER BY 1, 2
            ''')

    fig = px.scatter(df, x='online_at', y='total_jobs', trendline='rolling', trendline_options=dict(window=7),
                     title=f'Number of jobs online (7-day rolling average) in last {months_as_text}')

    main_graph = dcc.Graph(figure=fig)

    compare_graphs = {}
    for filter_name, df in compare_df.items():
        fig = px.line(df, x="online_at", y="total_jobs", color=filter_name, title=filter_name)
        compare_graphs[filter_name] = dcc.Graph(figure=fig)

    location_graph = compare_graphs['location_name'] if 'location_name' in compare_graphs.keys() else ''
    company_graph = compare_graphs['company_name'] if 'company_name' in compare_graphs.keys() else ''
    technology_graph = compare_graphs['technology_name'] if 'technology_name' in compare_graphs.keys() else ''

    elapsed_time = time.time() - start_time

    return [
        html.Div([main_graph]),
        html.Div([location_graph]),
        html.Div([company_graph]),
        html.Div([technology_graph]),
        html.Div(f'It took {elapsed_time:.2f} seconds'),
        options['location_name'],
        options['company_name'],
        options['technology_name'],
    ]


if __name__ == '__main__':
    is_debug = sys.argv[1].lower() != 'prod' if len(sys.argv) > 1 else True
    port = sys.argv[2] if len(sys.argv) > 2 else 8050

    app.run(host='localhost', port=port, debug=is_debug)

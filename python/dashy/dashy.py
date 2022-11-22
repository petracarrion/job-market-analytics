import functools
import json
import os
import sys
import time
import urllib.parse
from datetime import date
from threading import Lock

import dash_bootstrap_components as dbc
import duckdb
import plotly.express as px
from dash import Dash, dcc, html, Output, Input, State
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

LOG_FOLDER = os.getenv('LOG_FOLDER')
LOG_FILE = os.path.join(LOG_FOLDER, 'dashy.log')
logger.add(LOG_FILE, rotation="1 day")

DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')

LOADING_TYPE = 'dot'

GRAPH_CONFIG = {
    'staticPlot': True,
}

TIME_OPTIONS = [
    {'label': 'Last Month', 'value': '1'},
    {'label': 'Last Quarter', 'value': '3'},
    {'label': 'Last Year', 'value': '12'},
]

GRAPH_OPTIONS = [
    {'label': 'Start Y-Axis at Zero', 'value': 'startatzero'},
]

LOCK = Lock()


class Filter:
    def __init__(self, name, label):
        self.name = name
        self.label = label


FILTER_NAMES = ['location_name', 'company_name', 'technology_name']

FILTERS = {
    'location_name': Filter('location_name', 'City'),
    'company_name': Filter('company_name', 'Company'),
    'technology_name': Filter('technology_name', 'Technology'),
}

app = Dash('Dashy', title='Job Market Analytics', external_stylesheets=[dbc.themes.SANDSTONE])
server = app.server

time_selector = dbc.RadioItems(
    options=TIME_OPTIONS,
    value='1',
    id='time-selector',
    inline=True,
)

controls = dbc.Card(
    [
        html.Div([
            html.H3("Time Range"),
            dcc.Loading(
                id='loading-time-selector',
                children=[
                    time_selector,
                ],
                type=LOADING_TYPE,
            ),
        ]),
        html.Br(),
        html.Div([
            html.H3('City'),
            dcc.Loading(
                id='loading-location-selector',
                children=[
                    dcc.Dropdown(
                        options=[],
                        id='location-selector',
                        multi=True
                    ),
                ],
                type=LOADING_TYPE,
            ),
        ]),
        html.Br(),
        html.Div([
            html.H3('Company'),
            dcc.Loading(
                id='loading-company-selector',
                children=[
                    dcc.Dropdown(
                        options=[],
                        id='company-selector',
                        multi=True
                    ),
                ],
                type=LOADING_TYPE,
            ),
        ]),
        html.Br(),
        html.Div([
            html.H3('Technology'),
            dcc.Loading(
                id='loading-technology-selector',
                children=[
                    dcc.Dropdown(
                        options=[],
                        id='technology-selector',
                        multi=True
                    ),
                ],
                type=LOADING_TYPE,
            ),
        ]),
        html.Br(),
        html.Div([
            html.H3('Graph Options'),
            dcc.Loading(
                id='loading-graphoptions-selector',
                children=[
                    dbc.Checklist(
                        options=GRAPH_OPTIONS,
                        value=[],
                        id='graphoptions-selector',
                    ),
                ],
                type=LOADING_TYPE,
            ),
        ]),
    ],
    body=True,
)

app.layout = dbc.Container(
    [
        dcc.Location(id='url'),
        html.Br(),
        html.A([
            html.H1("Job Market Analytics"),
        ], href='/'),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col([
                    controls,
                ], md=4),
                dbc.Col(html.Div(
                    [
                        html.Br(),
                        html.H3("Number of jobs online"),
                        dcc.Loading(
                            id='loading-main-graph',
                            children=[html.Div(id='main-graph')],
                            type=LOADING_TYPE,
                        ),
                        dcc.Loading(
                            id='loading-location-graph',
                            children=[html.Div(id='location-graph')],
                            type=LOADING_TYPE,
                        ),
                        dcc.Loading(
                            id='loading-company-graph',
                            children=[html.Div(id='company-graph')],
                            type=LOADING_TYPE,
                        ),
                        dcc.Loading(
                            id='loading-technology-graph',
                            children=[html.Div(id='technology-graph')],
                            type=LOADING_TYPE,
                        ),
                        html.Br(),
                        dcc.Loading(
                            id='loading-performance-info',
                            children=[html.Div(id='performance-info')],
                            type=LOADING_TYPE,
                        ),
                    ]), md=8),
            ],
        ),
    ],
    fluid=True,
)


def encode_param(value):
    if isinstance(value, list):
        value = sorted(value)
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


@functools.lru_cache(maxsize=1024)
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
    Output('graphoptions-selector', 'value'),
    Input('url', 'pathname'),
    State('url', 'hash'),
    State('time-selector', 'value'),
)
def update_intial_values(_, url_hash, time_input):
    time_output = decode_params(url_hash, 'months') or time_input
    location_output = decode_params(url_hash, 'city')
    company_output = decode_params(url_hash, 'company')
    technology_output = decode_params(url_hash, 'technology')
    graphoptions_output = decode_params(url_hash, 'graphoptions')

    outputs = [time_output, location_output, company_output, technology_output, graphoptions_output]
    logger.info(f'update_intial_values: {outputs}')

    return outputs


@app.callback(
    Output('url', 'hash'),
    Input('time-selector', 'value'),
    Input('location-selector', 'value'),
    Input('company-selector', 'value'),
    Input('technology-selector', 'value'),
    Input('graphoptions-selector', 'value'),
)
def update_hash(time_input, location_input, company_input, technology_input, graphoptions_input):
    params = {
        'months': encode_param(time_input),
        'city': encode_param(location_input),
        'company': encode_param(company_input),
        'technology': encode_param(technology_input),
        'graphoptions': encode_param(graphoptions_input),
    }
    logger.info(f'update_hash: {params}')
    params = {k: v for k, v in params.items() if v}
    url_hash = urllib.parse.urlencode(params)
    return url_hash


@app.callback(
    Output('main-graph', 'children'),
    Output('location-graph', 'children'),
    Output('company-graph', 'children'),
    Output('technology-graph', 'children'),
    Output('performance-info', 'children'),
    Output('time-selector', 'options'),
    Output('location-selector', 'options'),
    Output('company-selector', 'options'),
    Output('technology-selector', 'options'),
    Output('graphoptions-selector', 'options'),
    Input('url', 'hash'),
)
def update_graphs(url_hash):
    with LOCK:
        start_time = time.time()

        inputs = {
            'time_name': decode_params(url_hash, 'months') or 1,
            'location_name': decode_params(url_hash, 'city'),
            'company_name': decode_params(url_hash, 'company'),
            'technology_name': decode_params(url_hash, 'technology'),
            'graphoptions_name': decode_params(url_hash, 'graphoptions'),
        }

        logger.info(f'update_graphs start: {inputs}')

        start_at_zero = False
        if inputs['graphoptions_name'] and 'startatzero' in inputs['graphoptions_name']:
            start_at_zero = True

        table_name = f'normalized_online_job_months_{inputs["time_name"]}'

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
                    SELECT {filter_name},
                           CAST(MAX(total_jobs) AS INTEGER) AS total_jobs
                    FROM (
                        SELECT {filter_name},
                               online_at,
                               COUNT(DISTINCT job_id) AS total_jobs
                          FROM {table_name}
                         WHERE {main_where_clause}
                         GROUP BY 1, 2
                    )
                    GROUP BY 1
                    ORDER BY 2 DESC
                ), cartesian_product AS (
                    SELECT d.online_at,
                           fo.{filter_name},
                           fo.total_jobs
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
                       CONCAT(cp.{filter_name}, ' (', cp.total_jobs, ')') AS {filter_name},
                       COALESCE(tj.total_jobs, 0) AS total_jobs
                  FROM cartesian_product cp
                  FULL OUTER JOIN total_jobs tj ON (
                       cp.online_at = tj.online_at AND
                       cp.{filter_name} = tj.{filter_name}
                   )
                 ORDER BY 1
                ''')

        fig = px.scatter(df, x='online_at', y='total_jobs', trendline='rolling', trendline_options=dict(window=7))
        if start_at_zero:
            fig.update_yaxes(rangemode='tozero')

        main_graph = html.Div([
            html.Br(),
            html.H5('Overview'),
            dcc.Graph(figure=fig, config=GRAPH_CONFIG)
        ])

        compare_graphs = {}
        for filter_name, df in compare_df.items():
            filter = FILTERS[filter_name]
            df = df.rename(columns={filter_name: filter.label})
            fig = px.line(df, x="online_at", y="total_jobs", color=filter.label)
            if start_at_zero:
                fig.update_yaxes(rangemode='tozero')
            compare_graphs[filter_name] = html.Div([
                html.Br(),
                html.H5(f'By {filter.label}'),
                dcc.Graph(figure=fig, config=GRAPH_CONFIG),
            ])

        location_graph = compare_graphs['location_name'] if 'location_name' in compare_graphs.keys() else ''
        company_graph = compare_graphs['company_name'] if 'company_name' in compare_graphs.keys() else ''
        technology_graph = compare_graphs['technology_name'] if 'technology_name' in compare_graphs.keys() else ''

        elapsed_time = time.time() - start_time
        elapsed_time = f'It took {elapsed_time:.2f} seconds on the backend'

        logger.info(f'update_graphs end: {elapsed_time}')

        return [
            html.Div([main_graph]),
            html.Div([location_graph]),
            html.Div([company_graph]),
            html.Div([technology_graph]),
            html.Div(elapsed_time),
            TIME_OPTIONS,
            options['location_name'],
            options['company_name'],
            options['technology_name'],
            GRAPH_OPTIONS,
        ]


if __name__ == '__main__':
    is_debug = sys.argv[1].lower() != 'prod' if len(sys.argv) > 1 else True
    port = sys.argv[2] if len(sys.argv) > 2 else 8050

    app.run(host='localhost', port=port, debug=is_debug)

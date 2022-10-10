import os
import sys

import dash_bootstrap_components as dbc
import duckdb
import plotly.express as px
from dash import Dash, dcc, html, Output, Input
from dotenv import load_dotenv

load_dotenv()

DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')

FILTER_NAMES = ['location_name', 'company_name']

app = Dash('Dashy', title='Job Market Analytics', external_stylesheets=[dbc.themes.SANDSTONE])
server = app.server

time_selector = dbc.RadioItems(
    options=[
        {'label': 'Last Year', 'value': '12'},
        {'label': 'Last 6 Months', 'value': '6'},
        {'label': 'Last 3 Months', 'value': '3'},
    ],
    value='12',
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
    ],
    body=True,
)

app.layout = dbc.Container(
    [
        html.H1("Job Market Analytics"),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(controls, md=4),
                dbc.Col(html.Div(id='main-graph'), md=8),
            ],
            align='center'
        ),
    ],
    fluid=True,
)

@app.callback(
    Output('main-graph', 'children'),
    Output('location-selector', 'options'),
    Output('company-selector', 'options'),
    Input('time-selector', 'value'),
    Input('location-selector', 'value'),
    Input('company-selector', 'value'),
)
def get_main_graph(time_input, location_input, company_input):
    _conn = duckdb.connect(DUCKDB_DWH_FILE, read_only=True)

    time_clause = f'online_at >= current_date - INTERVAL {time_input} MONTH' if time_input else '1 = 1'
    location_clause = f'location_name IN (SELECT UNNEST({location_input}))' if location_input else '1 = 1'
    company_clause = f'company_name IN (SELECT UNNEST({company_input}))' if company_input else '1 = 1'

    df = _conn.execute(f'''
    SELECT online_at,
           SUM(total_jobs) AS total_jobs
      FROM aggregated_online_job
     WHERE {time_clause} AND
           {location_clause} AND
           {company_clause}
     GROUP BY 1
     ORDER BY 1
    ''').df()

    filter_df = {}
    options = {}
    for filter_name in FILTER_NAMES:
        filter_df[filter_name] = _conn.execute(f'''
            SELECT {filter_name},
                   CAST(MAX(total_jobs) AS INTEGER) AS total_jobs
            FROM (
                SELECT {filter_name},
                       online_at,
                       SUM(total_jobs) AS total_jobs
                  FROM aggregated_online_job
                 WHERE {time_clause} AND
                       {location_clause if filter_name == 'company_name'  else '1 == 1'} AND
                       {company_clause  if filter_name == 'location_name' else '1 == 1'}
                 GROUP BY 1, 2
            )
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10000
            ''').df()

        filter_df_records = filter_df[filter_name].to_dict('records')
        options[filter_name] = [
            {'label': f'{option[filter_name]} ({option["total_jobs"]})', 'value': option[filter_name]}
            for option in filter_df_records
        ]

    _conn.close()

    fig = px.scatter(df, x='online_at', y='total_jobs', trendline='rolling', trendline_options=dict(window=7),
                     title='Number of jobs online (7-day rolling average)')

    main_graph = dcc.Graph(figure=fig)

    return [
        html.Div([main_graph]),
        options['location_name'],
        options['company_name'],
    ]


if __name__ == '__main__':
    is_debug = sys.argv[1].lower() != 'prod' if len(sys.argv) > 1 else False
    port = sys.argv[2] if len(sys.argv) > 2 else 8050

    app.run(host='localhost', port=port, debug=is_debug)

import os

import dash_bootstrap_components as dbc
import duckdb
import plotly.express as px
from dash import Dash, dcc, html, Output, Input
from dotenv import load_dotenv

load_dotenv()

DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')

app = Dash('Dashy', title='Job Market Analytics', external_stylesheets=[dbc.themes.SANDSTONE])


@app.callback(Output('main-graph', 'children'),
              Input('time-selector', 'value'),
              Input('location-selector', 'value'),
              Input('company-selector', 'value'))
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

    _conn.close()

    fig = px.scatter(df, x='online_at', y='total_jobs', trendline='rolling', trendline_options=dict(window=7),
                     title='Number of jobs online (7-day rolling average)')

    main_graph = dcc.Graph(figure=fig)

    return html.Div([
        main_graph
    ])


if __name__ == '__main__':
    conn = duckdb.connect(DUCKDB_DWH_FILE, read_only=True)

    df_top_cities = conn.execute('''
    SELECT location_name,
           SUM(total_jobs) AS total_jobs
      FROM aggregated_online_job
     GROUP BY 1
     ORDER BY 2 DESC
    LIMIT 5000
    ''').df()

    df_top_companies = conn.execute('''
    SELECT company_name,
           SUM(total_jobs) AS total_jobs
      FROM aggregated_online_job
     GROUP BY 1
     ORDER BY 2 DESC
    LIMIT 5000
    ''').df()

    conn.close()

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

    location_options = df_top_cities['location_name'].to_list()
    location_options = [{'label': location, 'value': location} for location in location_options]
    location_selector = dcc.Dropdown(
        options=location_options,
        id='location-selector',
        multi=True
    )

    company_options = df_top_companies['company_name'].to_list()
    company_options = [{'label': company, 'value': company} for company in company_options]
    company_selector = dcc.Dropdown(
        options=company_options,
        id='company-selector',
        multi=True
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
                location_selector
            ]),
            html.Br(),
            html.Div([
                html.H3('Company'),
                company_selector
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

    app.run_server(debug=True)

import os

import duckdb
import plotly.express as px
from dash import Dash, dcc, html
from dotenv import load_dotenv

if __name__ == '__main__':

    load_dotenv()

    DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')

    conn = duckdb.connect(DUCKDB_DWH_FILE, read_only=True)

    df = conn.execute('''
    SELECT online_at,
           SUM(total_jobs) AS total_jobs
      FROM aggregated_online_job
     GROUP BY 1
    ORDER BY 1
    ''').df()

    df_top_cities = conn.execute('''
    SELECT location_name,
           SUM(total_jobs) AS total_jobs
      FROM aggregated_online_job
     GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 100
    ''').df()

    df_top_companies = conn.execute('''
    SELECT company_name,
           SUM(total_jobs) AS total_jobs
      FROM aggregated_online_job
     GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 100
    ''').df()

    conn.close()

    time_selector = dcc.RadioItems(
        options=[
            {'label': 'Last Year', 'value': '12'},
            {'label': 'Last 6 Months', 'value': '6'},
            {'label': 'Last 3 Months', 'value': '3'},
        ],
        value='12'
    )

    city_options = df_top_cities['location_name'].to_list()
    city_options = [{'label': city, 'value': city} for city in city_options]
    city_selector = dcc.Checklist(
        options=city_options
    )

    company_options = df_top_companies['company_name'].to_list()
    company_options = [{'label': company, 'value': company} for company in company_options]
    company_selector = dcc.Checklist(
        options=company_options
    )

    fig = px.scatter(df, x='online_at', y='total_jobs', trendline='rolling', trendline_options=dict(window=7),
                     title='Number of jobs online (7-day rolling average)')

    app = Dash('Dashy', title='Job Market Analytics')
    app.layout = html.Div([
        html.H1('Job Market Analytics'),
        html.Div([
            html.H3('Time Range'),
            time_selector
        ]),
        html.Div([
            html.H3('City'),
            city_selector
        ]),
        html.Div([
            html.H3('Company'),
            company_selector
        ]),
        dcc.Graph(figure=fig)
    ])

    app.run_server(debug=True)

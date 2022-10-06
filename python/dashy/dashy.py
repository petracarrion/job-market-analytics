import os

import duckdb
import plotly.express as px
from dash import Dash, dcc, html, Output, Input, ctx
from dotenv import load_dotenv

load_dotenv()

DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')

app = Dash('Dashy', title='Job Market Analytics')


@app.callback(Output('main-graph', 'children'),
              Input('time-selector', 'value'),
              Input('location-selector', 'value'),
              Input('company-selector', 'value'))
def get_main_graph(time_input, location_input, company_input):
    _conn = duckdb.connect(DUCKDB_DWH_FILE, read_only=True)

    time_clause = f'online_at >= current_date - INTERVAL {time_input} MONTH' if time_input else '1 = 1'
    location_clause = f'location_name IN (SELECT UNNEST({location_input}))' if location_input else '1 = 1'
    company_clause = f'company_name IN (SELECT UNNEST({company_input}))' if company_input else '1 = 1'

    # if location_input:
    #     joined_locations = ', '.join(location_input)
    #     location_clause = f'location_name in ({joined_locations})'
    # else:
    #     location_clause = '1 = 1'
    #
    # if company_input:
    #     joined_companies = ', '.join(company_input)
    #     company_clause = f'location_name in ({joined_companies})'
    # else:
    #     company_clause = '1 = 1'

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
        value='12',
        id='time-selector'
    )

    location_options = df_top_cities['location_name'].to_list()
    location_options = [{'label': location, 'value': location} for location in location_options]
    location_selector = dcc.Checklist(
        options=location_options,
        id='location-selector'
    )

    company_options = df_top_companies['company_name'].to_list()
    company_options = [{'label': company, 'value': company} for company in company_options]
    company_selector = dcc.Checklist(
        options=company_options,
        id='company-selector'
    )

    app.layout = html.Div([
        html.H1('Job Market Analytics'),
        html.Div([
            html.H3('Time Range'),
            time_selector
        ]),
        html.Div([
            html.H3('City'),
            location_selector
        ]),
        html.Div([
            html.H3('Company'),
            company_selector
        ]),
        html.Div(id='main-graph')
    ])

    app.run_server(debug=True)

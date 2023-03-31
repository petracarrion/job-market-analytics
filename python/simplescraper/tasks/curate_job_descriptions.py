import sys

import numpy as np

from common.entity import JOB, JOB_LOCATION, JOB_DESCRIPTION, JOB_TECHNOLOGY
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_load_date, load_cleansed_df, save_curated_df

JOB_DESCRIPTION_SAT_COLUMNS = ['title', 'online_status', 'is_anonymous', 'should_display_early_applicant',
                               'contract_type', 'work_type', 'online_date', 'company_name', 'description_introduction',
                               'description_responsabilities', 'description_requirements', 'description_perks']

BASE_COLUMNS = ['year', 'month', 'day', 'job_id', 'load_timestamp']

TECHNOLOGIES = [
    'AI',
    'Airflow',
    'Android',
    'Angular',
    'AWS',
    'Azure',
    'CSS',
    'Couchbase',
    'CouchDB',
    'Cypress',
    'Dagster',
    'Dask',
    'Databricks',
    'dbt',
    'Docker',
    'Duckdb',
    'ELT',
    'ETL',
    'Flink',
    'Flutter',
    'GCP',
    'Go',
    'Golang',
    'Gradle',
    'gRPC',
    'HANA',
    'Java',
    'JavaScript',
    'Keras',
    'Kotlin',
    'Kubernetes',
    'LESS',
    'Maven',
    'ML',
    'MongoDB',
    'MySQL',
    'NLP',
    'Oracle',
    'Pandas',
    'Playwright',
    'PostgreSQL',
    'Prefect',
    'Puppeteer',
    'Purview',
    'Python',
    'PyTorch',
    'React',
    'REST',
    'Rust',
    'Tensorflow',
    'TestCafe',
    'TypeScript',
    'WebAssembly',
    'scikit',
    'Selenium',
    'Snowflake',
    'Snowplow',
    'Spark',
    'Spring',
    'Storm',
    'SAP',
    'SCSS',
    'SQL',
    'SSIS',
    'Synapse',
    'Vue',
]


def process_job_description(df):
    df = df.copy()
    df = df[df['company_name'].notna()]
    df = df[BASE_COLUMNS + JOB_DESCRIPTION_SAT_COLUMNS]
    save_curated_df(df, JOB)


def process_location(df):
    df = df[BASE_COLUMNS + ['location']].copy()

    df['location'] = df['location'].str.replace('Frankfurt (Main)', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt a. M.', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt a.M.', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt am Main (60488)', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt Am Main', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt/M.', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt aM', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt (am Main)', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt Main', 'Frankfurt am Main', regex=False)
    df['location'] = df['location'].str.replace('Frankfurt aam Main', 'Frankfurt am Main', regex=False)

    df['location'] = df['location'].str.replace('|'.join([' und ', ' oder ', '/', ';', ' - ', ':']), ',', regex=True)
    df['location'] = df['location'].str.replace(' | ', ',', regex=False)
    df['location'] = df['location'].str.replace(' .', ',', regex=False)
    df['location'] = df['location'].str.replace(' u.a. ', ',', regex=False)
    df['location'] = df['location'].str.split(',')
    df = df.explode('location').reset_index(drop=True)

    df['location'] = df['location'].str.strip()

    df['location'] = df['location'].replace('Frankfurt', 'Frankfurt am Main')

    df['location'] = df['location'].replace('', np.nan)
    df['location'] = df['location'].replace('keine Angabe', np.nan)
    df = df.dropna()

    save_curated_df(df, JOB_LOCATION)


def process_technology(df):
    df = df.copy()
    df['description'] = df['title'] + ' ' + \
                        df['description_introduction'] + ' ' + \
                        df['description_responsabilities'] + ' ' + \
                        df['description_requirements'] + ' ' + \
                        df['description_perks']
    for technology in TECHNOLOGIES:
        df[technology] = df['description'].str.contains(fr'(?i)\b{technology}\b', regex=True)
    df['Other'] = ~df[TECHNOLOGIES].any(axis='columns')
    df = df.melt(id_vars=BASE_COLUMNS, value_vars=TECHNOLOGIES + ['Other'], var_name='technology')
    df = df[df['value'].notna()]
    df = df[df['value']]
    df = df[BASE_COLUMNS + ['technology']]

    save_curated_df(df, JOB_TECHNOLOGY)


def curate_job_descriptions(load_timestamp, load_date):
    configure_logger(load_timestamp)
    logger.info(f'Start curate_job_descriptions: {load_timestamp} {load_date}')

    df = load_cleansed_df(JOB_DESCRIPTION, load_date=load_date)

    df = df.dropna(subset=['job_id'])
    df['job_id'] = df['job_id'].astype('int')
    df = df.sort_values(by=['job_id'])

    process_job_description(df)
    process_location(df)
    process_technology(df)

    logger.info(f'End   curate_job_descriptions: {load_timestamp} {load_date}')


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    curate_job_descriptions(_load_timestamp, _load_date)

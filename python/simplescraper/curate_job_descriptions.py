import sys

import numpy as np

from common.entity import JOB_DESCRIPTION, JOB, COMPANY, JOB_COMPANY, JOB_LOCATION, LOCATION
from common.logging import configure_logger, logger
from common.storage import get_run_timestamp, get_target_date, load_cleansed_df, save_curated_df


def process_job(df):
    df = df[['year', 'month', 'day', 'job_id']]
    save_curated_df(df, JOB)


def process_company(df):
    df = df[['year', 'month', 'day', 'job_id', 'company_name']]
    save_curated_df(df, JOB_COMPANY)

    df = df[['year', 'month', 'day', 'company_name']]
    df = df.drop_duplicates()
    df = df.sort_values(by=['company_name'])
    save_curated_df(df, COMPANY)


def process_location(df):
    df = df[['year', 'month', 'day', 'job_id', 'location']].copy()

    df['location'] = df['location'].str.replace(' und ', ',')
    df['location'] = df['location'].str.replace(' oder ', ',')
    df['location'] = df['location'].str.replace(' u.a. ', ',', regex=False)
    df['location'] = df['location'].str.replace('/', ',')
    df['location'] = df['location'].str.split(',')
    df = df.explode('location').reset_index(drop=True)

    df['location'] = df['location'].str.strip()
    df['location'] = df['location'].replace('', np.nan)
    df['location'] = df['location'].replace('keine Angabe', np.nan)
    df = df.dropna()

    save_curated_df(df, JOB_LOCATION)

    df = df[['year', 'month', 'day', 'location']].copy()
    df = df.drop_duplicates()
    df = df.sort_values(by=['location'])
    save_curated_df(df, LOCATION)


def curate_job_descriptions(run_timestamp, target_date):
    configure_logger(run_timestamp, 'curate_job_descriptions')
    logger.info(f'Start curate_job_descriptions: {target_date}')
    df = load_cleansed_df(JOB_DESCRIPTION, target_date=target_date)
    df = df.sort_values(by=['job_id'])

    process_job(df)
    process_company(df)
    process_location(df)

    logger.info(f'End   curate_job_descriptions: {target_date}')


if __name__ == "__main__":
    _run_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_run_timestamp()
    _target_date = sys.argv[2] if len(sys.argv) > 2 else get_target_date()
    curate_job_descriptions(_run_timestamp, _target_date)

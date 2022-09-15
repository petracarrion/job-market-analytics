import sys

import numpy as np

from common.entity import JOB_DESCRIPTION, JOB_COMPANY, JOB_LOCATION
from common.hashing import hash_columns
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_load_date, load_cleansed_df, save_curated_df

JOB_DESCRIPTION_SAT_COLUMNS = ['title', 'online_status', 'is_anonymous', 'should_display_early_applicant',
                               'contract_type', 'work_type', 'online_date', 'description_introduction',
                               'description_responsabilities', 'description_requirements', 'description_perks']

BASE_COLUMNS = ['year', 'month', 'day', 'job_id', 'load_timestamp']


def process_job_description(df):
    df = df.copy()
    df = df[BASE_COLUMNS + JOB_DESCRIPTION_SAT_COLUMNS]
    df['job_hashdiff'] = hash_columns(df, JOB_DESCRIPTION_SAT_COLUMNS)
    save_curated_df(df, JOB_DESCRIPTION)


def process_company(df):
    df = df[BASE_COLUMNS + ['company_name']].copy()
    df = df.rename(columns={'company_name': 'company'})
    save_curated_df(df, JOB_COMPANY)


def process_location(df):
    df = df[BASE_COLUMNS + ['location']].copy()

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


def curate_job_descriptions(load_timestamp, load_date):
    configure_logger(load_timestamp, 'curate_job_descriptions')
    logger.info(f'Start curate_job_descriptions: {load_date}')
    df = load_cleansed_df(JOB_DESCRIPTION, load_date=load_date)
    df = df.dropna(subset=['job_id'])
    df = df.sort_values(by=['job_id'])

    process_job_description(df)
    process_company(df)
    process_location(df)

    logger.info(f'End   curate_job_descriptions: {load_date}')


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    curate_job_descriptions(_load_timestamp, _load_date)

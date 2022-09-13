import sys

import numpy as np

from common.entity import JOB_DESCRIPTION, JOB_COMPANY, JOB_LOCATION
from common.hashing import hash_str, hash_columns
from common.logging import configure_logger, logger
from common.storage import get_run_timestamp, get_target_date, load_cleansed_df, save_curated_df

JOB_DESCRIPTION_SAT_COLUMNS = ['title', 'online_status', 'is_anonymous', 'should_display_early_applicant',
                               'contract_type', 'work_type', 'online_date', 'description_introduction',
                               'description_responsabilities', 'description_requirements', 'description_perks']

BASE_COLUMNS = ['year', 'month', 'day', 'job_id', 'job_hk']


def process_job_description(df):
    df = df.copy()
    df = df[BASE_COLUMNS + JOB_DESCRIPTION_SAT_COLUMNS]
    df['job_hashdiff'] = hash_columns(df, JOB_DESCRIPTION_SAT_COLUMNS)
    df = df.rename(columns={'online_date': 'online_since'})
    save_curated_df(df, JOB_DESCRIPTION)


def process_company(df):
    df = df[BASE_COLUMNS + ['company_name']].copy()
    df = df.rename(columns={'company_name': 'company'})
    df['company_hk'] = df['company'].astype(str).apply(hash_str)
    save_curated_df(df, JOB_COMPANY)


def process_location(df):
    df = df[BASE_COLUMNS + ['location']].copy()
    df['location_hk'] = df['location'].astype(str).apply(hash_str)

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


def curate_job_descriptions(run_timestamp, target_date):
    configure_logger(run_timestamp, 'curate_job_descriptions')
    logger.info(f'Start curate_job_descriptions: {target_date}')
    df = load_cleansed_df(JOB_DESCRIPTION, target_date=target_date)
    df = df.sort_values(by=['job_id'])
    df['job_hk'] = df['job_id'].astype(str).apply(hash_str)

    process_job_description(df)
    process_company(df)
    process_location(df)

    logger.info(f'End   curate_job_descriptions: {target_date}')


if __name__ == "__main__":
    _run_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_run_timestamp()
    _target_date = sys.argv[2] if len(sys.argv) > 2 else get_target_date()
    curate_job_descriptions(_run_timestamp, _target_date)

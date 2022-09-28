import sys

import pandas as pd

from common.entity import SITEMAP, ONLINE_JOB
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_load_date, load_cleansed_df, save_curated_df
from tasks.curate_job_descriptions import BASE_COLUMNS

ONLINE_JOB_SAT_COLUMNS = ['online_at', 'url']


def curate_sitemaps(load_timestamp, load_date):
    configure_logger(load_timestamp)
    logger.info(f'Start curate_sitemaps: {load_timestamp} {load_date}')

    df = load_cleansed_df(SITEMAP, load_date=load_date)

    df = df.dropna(subset=['job_id'])
    df['job_id'] = df['job_id'].astype('int')
    df['online_at'] = pd.to_datetime(df['load_timestamp']).dt.date
    df = df[BASE_COLUMNS + ONLINE_JOB_SAT_COLUMNS]
    df = df.sort_values(by=['job_id'])

    save_curated_df(df, ONLINE_JOB)
    logger.info(f'End   curate_sitemaps: {load_timestamp} {load_date}')


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    curate_sitemaps(_load_timestamp, _load_date)

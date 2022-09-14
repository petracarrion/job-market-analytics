import sys

import pandas as pd

from common.entity import SITEMAP, JOB_ONLINE
from common.hashing import hash_columns
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_target_date, load_cleansed_df, save_curated_df
from curate_job_descriptions import BASE_COLUMNS

JOB_ONLINE_SAT_COLUMNS = ['online_at', 'url']


def curate_sitemaps(load_timestamp, target_date):
    configure_logger(load_timestamp, 'curate_sitemaps')
    logger.info(f'Start curate_sitemaps: {target_date}')
    df = load_cleansed_df(SITEMAP, target_date=target_date)

    df['online_at'] = pd.to_datetime(df['load_timestamp']).dt.date
    df = df[BASE_COLUMNS + JOB_ONLINE_SAT_COLUMNS]
    df['job_online_hashdiff'] = hash_columns(df, JOB_ONLINE_SAT_COLUMNS)
    df = df.sort_values(by=['job_id'])

    save_curated_df(df, JOB_ONLINE)
    logger.info(f'End   curate_sitemaps: {target_date}')


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _target_date = sys.argv[2] if len(sys.argv) > 2 else get_target_date()
    curate_sitemaps(_load_timestamp, _target_date)

import sys

import pandas as pd

from common.entity import SITEMAP, JOB_ONLINE
from common.logging import configure_logger
from common.storage import get_run_timestamp, get_target_date, load_cleansed_df, save_curated_df, split_target_date


def list_cleansed_sitemaps(target_date):
    year, month, day = split_target_date(target_date)
    filters = [
        ('year', '=', int(year)),
        ('month', '=', int(month)),
        ('day', '=', int(day)),
    ]
    df = load_cleansed_df(SITEMAP, filters=filters)
    return df


def curate_sitemaps(run_timestamp, target_date):
    logger = configure_logger(run_timestamp, 'curate_sitemaps')
    df = list_cleansed_sitemaps(target_date)
    df['online_date'] = pd.to_datetime(df['run_timestamp']).dt.date
    df = df[['year', 'month', 'day', 'job_id', 'online_date', 'url']]
    df = df.sort_values(by=['job_id'])
    logger.info(f'Saving curated: {target_date}')
    save_curated_df(df, JOB_ONLINE)


if __name__ == "__main__":
    _run_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_run_timestamp()
    _target_date = sys.argv[2] if len(sys.argv) > 2 else get_target_date()
    curate_sitemaps(_run_timestamp, _target_date)

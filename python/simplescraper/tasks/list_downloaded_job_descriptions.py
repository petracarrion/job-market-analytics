import datetime
import functools

import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import LATEST_RUN_TIMESTAMP, ONLINE_EXPIRATION_IN_DAYS
from common.logging import logger
from common.storage import DOWNLOADED_JOB_DESCRIPTIONS_CSV, DATA_SOURCE_NAME, save_temp_df, list_raw_files


@functools.lru_cache(maxsize=None)
def calculate_days_online(run_timestamp):
    ingestion_datetime = datetime.datetime.strptime(run_timestamp, '%Y/%m/%d/%H-%M-%S')
    now = datetime.datetime.now()
    delta = now - ingestion_datetime
    return delta.days


def list_downloaded_job_descriptions(run_timestamp) -> pd.DataFrame:
    logger.info('list_downloaded_job_descriptions start')
    files = list_raw_files(DATA_SOURCE_NAME, JOB_DESCRIPTION)
    df = pd.DataFrame(files)
    df['id'] = df['file_name'].str.split('.', expand=True)[0]
    if ONLINE_EXPIRATION_IN_DAYS:
        df['days_online'] = df['run_timestamp'].map(calculate_days_online)
        df = df[df['days_online'] < ONLINE_EXPIRATION_IN_DAYS]
        df = df.drop(columns=['days_online'])
    save_temp_df(df, run_timestamp, DOWNLOADED_JOB_DESCRIPTIONS_CSV)
    logger.info('list_downloaded_job_descriptions end')
    return df


if __name__ == "__main__":
    list_downloaded_job_descriptions(LATEST_RUN_TIMESTAMP)

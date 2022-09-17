import datetime
import functools

import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import LATEST_LOAD_TIMESTAMP, ONLINE_EXPIRATION_IN_DAYS
from common.logging import logger, configure_logger
from common.storage import DOWNLOADED_JOB_DESCRIPTIONS_CSV, DATA_SOURCE_NAME, save_temp_df, list_raw_files


@functools.lru_cache(maxsize=None)
def calculate_days_online(load_timestamp):
    ingestion_datetime = datetime.datetime.strptime(load_timestamp, '%Y/%m/%d/%H-%M-%S')
    now = datetime.datetime.now()
    delta = now - ingestion_datetime
    return delta.days


def list_downloaded_job_descriptions(load_timestamp, load_date=None) -> pd.DataFrame:
    configure_logger(load_timestamp)
    logger.info('list_downloaded_job_descriptions start')
    files = list_raw_files(DATA_SOURCE_NAME, JOB_DESCRIPTION, load_date)
    df = pd.DataFrame(files)
    if not df.empty:
        df['id'] = df['file_name'].str.split('.', expand=True)[0]
        if ONLINE_EXPIRATION_IN_DAYS:
            df['days_online'] = df['load_timestamp'].map(calculate_days_online)
            df = df[df['days_online'] < ONLINE_EXPIRATION_IN_DAYS]
            df = df.drop(columns=['days_online'])
        if load_date is None:
            save_temp_df(df, load_timestamp, DOWNLOADED_JOB_DESCRIPTIONS_CSV)
    logger.info('list_downloaded_job_descriptions end')
    return df


if __name__ == "__main__":
    list_downloaded_job_descriptions(LATEST_LOAD_TIMESTAMP)

import datetime
import functools

import pandas as pd

from common.entity import RAW_ENTITIES
from common.env_variables import DATA_SOURCE_NAME, RAW_DIR
from common.storage import list_raw_days


def get_current_date():
    return datetime.datetime.today().strftime('%Y%m%d')


@functools.lru_cache(maxsize=None)
def calculate_days_online(load_timestamp):
    ingestion_datetime = datetime.datetime.strptime(load_timestamp, '%Y%m%d')
    now = datetime.datetime.now()
    delta = now - ingestion_datetime
    return delta.days


def list_missing_previous_dates(entity):
    df = pd.DataFrame(list_raw_days(DATA_SOURCE_NAME, entity))
    df_current_date = pd.DataFrame([{
        'date': get_current_date()
    }])
    df = df.drop_duplicates()
    df = pd.concat([
        df,
        df_current_date, df_current_date
    ]).drop_duplicates(keep=False)
    return df


def remove_old_files():
    dfs = []
    for entity in RAW_ENTITIES:
        df = list_missing_previous_dates(entity)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates()
    df['days_online'] = df['date'].map(calculate_days_online)
    # 2022-09-20 242
    # 2022-09-21 240
    df = df[df['days_online'] > 242]
    df = df.sort_values(by=['date'])
    dates_to_download = df['date'].to_list()
    for date_to_download in dates_to_download:
        year = date_to_download[:4]
        month = date_to_download[4:6]
        day = date_to_download[6:8]
        for entity in RAW_ENTITIES:
            print(f'rm -rf {RAW_DIR}/{DATA_SOURCE_NAME}/{entity}/{year}/{month}/{day}')


if __name__ == "__main__":
    remove_old_files()

import datetime

import pandas as pd

from common.entity import ALL_ENTITIES
from common.env_variables import DATA_SOURCE_NAME
from common.storage import list_raw_days


def get_current_date():
    return datetime.datetime.today().strftime('%Y%m%d')


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


def verify_backups():
    dfs = []
    for entity in ALL_ENTITIES:
        df = list_missing_previous_dates(entity)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates()
    df = df.sort_values(by=['date'])
    dates_to_download = df['date'].to_list()
    for date_to_download in dates_to_download:
        year = date_to_download[:4]
        month = date_to_download[4:6]
        day = date_to_download[6:8]
        print(
            f'/bin/zsh /Users/carrion/PycharmProjects/job-market-analytics/python/simplescraper/verify_day_backup.sh {year} {month} {day} || exit 1')


if __name__ == "__main__":
    verify_backups()

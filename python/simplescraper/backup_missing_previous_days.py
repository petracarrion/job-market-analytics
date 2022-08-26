import datetime

import pandas as pd

from common.entity import ALL_ENTITIES
from common.env_variables import DATA_SOURCE_NAME
from common.storage import list_raw_days, list_backup_days


def get_current_date():
    return datetime.datetime.today().strftime('%Y%m%d')


def list_missing_previous_dates(entity):
    df = pd.DataFrame(list_raw_days(DATA_SOURCE_NAME, entity))
    df_backup_days = pd.DataFrame(list_backup_days(DATA_SOURCE_NAME, entity))
    df_current_date = pd.DataFrame([{
        'date': get_current_date()
    }])
    df = df.drop_duplicates()
    df = pd.concat([
        df,
        df_backup_days, df_backup_days,
        df_current_date, df_current_date
    ]).drop_duplicates(keep=False)
    return df


def backup_missing_previous_days():
    dfs = []
    for entity in ALL_ENTITIES:
        print(entity)
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
            f'/bin/zsh /Users/carrion/PycharmProjects/job-market-analytics/python/simplescraper/backup_day.sh {year} {month} {day}')
    print(dates_to_download)


if __name__ == "__main__":
    backup_missing_previous_days()

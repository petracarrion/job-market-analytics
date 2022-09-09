import datetime

import pandas as pd

from common.entity import ALL_ENTITIES
from common.env_variables import DATA_SOURCE_NAME, SOURCE_DIR
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


def print_script_statements(script_name, dates_to_download):
    for date_to_download in dates_to_download:
        year = date_to_download[:4]
        month = date_to_download[4:6]
        day = date_to_download[6:8]
        print(
            f'/bin/zsh {SOURCE_DIR}/simplescraper/{script_name} {year} {month} {day}')


def backup_missing_previous_days():
    dfs = []
    for entity in ALL_ENTITIES:
        df = list_missing_previous_dates(entity)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates()
    df = df.sort_values(by=['date'])
    dates_to_download = df['date'].to_list()
    print_script_statements('backup_day.sh', dates_to_download)
    print()
    print_script_statements('verify_day_backup.sh', dates_to_download)


if __name__ == "__main__":
    backup_missing_previous_days()

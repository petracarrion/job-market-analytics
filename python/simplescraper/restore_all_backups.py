import datetime

import pandas as pd

from common.entity import RAW_ENTITIES
from common.env_variables import DATA_SOURCE_NAME, SOURCE_DIR
from common.storage import list_raw_days, list_backup_days


def get_current_date():
    return datetime.datetime.today().strftime('%Y%m%d')


def list_backups_to_restore(entity):
    df = pd.DataFrame(list_backup_days(DATA_SOURCE_NAME, entity))
    df_in_raw = pd.DataFrame(list_raw_days(DATA_SOURCE_NAME, entity))
    df_current_date = pd.DataFrame([{
        'date': get_current_date()
    }])
    df = df.drop_duplicates()
    df = pd.concat([
        df,
        df_in_raw, df_in_raw,
        df_current_date, df_current_date
    ]).drop_duplicates(keep=False)
    return df


def print_script_statements(script_name, days_to_restore):
    for day_to_restore in days_to_restore:
        year = day_to_restore[:4]
        month = day_to_restore[4:6]
        day = day_to_restore[6:8]
        print(
            f'/bin/zsh {SOURCE_DIR}/simplescraper/{script_name} {year} {month} {day} || exit 1')


def restore_all_backups():
    dfs = []
    for entity in RAW_ENTITIES:
        df = list_backups_to_restore(entity)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates()
    df = df.sort_values(by=['date'])
    days_to_restore = df['date'].to_list()
    print_script_statements('restore_day_backup.sh', days_to_restore)
    print()
    print_script_statements('verify_day_backup.sh', days_to_restore)


if __name__ == "__main__":
    restore_all_backups()

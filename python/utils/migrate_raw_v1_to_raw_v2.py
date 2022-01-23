import datetime
import glob
import os
import shutil

import pandas as pd

from common.env_variables import LATEST_RUN_TIMESTAMP, RAW_DIR, DATA_DIR
from common.storage import DATA_SOURCE_NAME, save_temp_df, load_temp_df


def list_raw_files(data_source):
    dir_path = os.path.join(RAW_DIR, data_source)
    file_list = [{
        'old_file_path': f,
        'entity': f.split('/')[-3],
        'timestamp': datetime.datetime.fromtimestamp(os.stat(f).st_birthtime),
        'file_name': f.split('/')[-1],
    } for f in glob.iglob(dir_path + '/*/*/*', recursive=True) if os.path.isfile(f)]
    return file_list


def list_downloaded_files(run_timestamp) -> pd.DataFrame:
    files = list_raw_files(DATA_SOURCE_NAME)
    df = pd.DataFrame(files)
    # df = df[df['file_name'] != 'sitemapindex.xml']
    save_temp_df(df, run_timestamp, '00_downloaded_raw_files.csv')
    return df


def timestamp_to_datatime_partition(timestamp):
    timestamp = str(timestamp)
    split1, split2 = timestamp.split()
    year, month, day = split1.split('-')
    hour = split2[:2]
    datatime_partition = f'{year}/{month}/{day}/{hour}-00-00'
    return datatime_partition


def get_new_file_path(row):
    new_file_path = os.path.join(DATA_DIR, 'raw_v2', DATA_SOURCE_NAME, row['entity'], row['datatime_partition'],
                                 row['file_name'])
    return new_file_path


def copy_file(row):
    src = row['old_file_path']
    dst = row['new_file_path']
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)


def copy_files_to_raw_v2(run_timestamp):
    df = load_temp_df(run_timestamp, '00_downloaded_raw_files.csv')
    df['datatime_partition'] = df['timestamp'].apply(timestamp_to_datatime_partition)
    df['new_file_path'] = df.apply(get_new_file_path, axis=1)
    df.apply(copy_file, axis=1)


if __name__ == "__main__":
    list_downloaded_files(LATEST_RUN_TIMESTAMP)
    copy_files_to_raw_v2(LATEST_RUN_TIMESTAMP)

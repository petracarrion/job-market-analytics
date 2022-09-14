import glob
import os
import shutil

import pandas as pd

from common.env_variables import LATEST_LOAD_TIMESTAMP, RAW_DIR
from common.storage import DATA_SOURCE_NAME, save_temp_df, load_temp_df


def list_raw_files(data_source, entity):
    dir_path = os.path.join(RAW_DIR, data_source, entity)
    file_list = [{
        'old_file_path': f,
    } for f in glob.iglob(dir_path + '/*/*/*/*/*', recursive=True) if os.path.isfile(f)]
    return file_list


def list_downloaded_files(load_timestamp) -> pd.DataFrame:
    files = list_raw_files(DATA_SOURCE_NAME, 'job_description')
    df = pd.DataFrame(files)
    save_temp_df(df, load_timestamp, '00_downloaded_raw_job_descriptions.csv')
    return df


def get_new_file_path(row):
    old_file_path = row['old_file_path']
    dirname = os.path.dirname(old_file_path)
    basename = os.path.basename(old_file_path)
    job_id = basename.rsplit('--', 1)
    job_id = job_id[1]
    job_id = job_id.split('-')
    job_id = job_id[0]
    new_file_path = os.path.join(dirname.replace('/raw/', '/raw_v3/'), f'{job_id}.html')
    return new_file_path


def copy_file(row):
    src = row['old_file_path']
    dst = row['new_file_path']
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)


def copy_files_to_raw_v2(load_timestamp):
    df = load_temp_df(load_timestamp, '00_downloaded_raw_job_descriptions.csv')
    df['new_file_path'] = df.apply(get_new_file_path, axis=1)
    df.apply(copy_file, axis=1)


if __name__ == "__main__":
    copy_files_to_raw_v2(LATEST_LOAD_TIMESTAMP)

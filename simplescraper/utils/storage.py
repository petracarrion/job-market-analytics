"""
This module will store the files in the following structure
- root
  - <layer>
    - <data_source>
      - <entity>
        - <timestamp>
          - <file.extension>
"""
import glob
import os
import pathlib
import datetime

import pandas as pd

from utils.env_variables import DATA_SOURCE_NAME, DATA_DIR

RAW_LAYER = 'raw'
CLEANSED_LAYER = 'cleansed'
CURATED_LAYER = 'curated'
TEMP_LAYER = 'temp'

LAYERS = [RAW_LAYER, CLEANSED_LAYER, CURATED_LAYER, TEMP_LAYER]

RAW_DIR = os.path.join(DATA_DIR, RAW_LAYER)
TEMP_DIR = os.path.join(DATA_DIR, TEMP_LAYER)

LAYER_DIR = {
    RAW_LAYER: RAW_DIR,
    TEMP_LAYER: TEMP_DIR,
}

DOWNLOADED_URLS_CSV = '01_downloaded_urls.csv'
SITEMAP_URLS_CSV = '02_sitemap_urls.csv'
URLS_TO_DOWNLOAD_CSV = '03_urls_to_download.csv'


def list_raw_files(data_source, entity):
    dir_path = os.path.join(RAW_DIR, data_source, entity)
    file_list = [f.split('/')[-1] for f in glob.iglob(dir_path + '/*/*', recursive=True) if os.path.isfile(f)]
    return file_list


def raw_files_exists(data_source, entity, file_name):
    dir_path = os.path.join(RAW_DIR, data_source, entity)
    file_list = [f.split('/')[-1] for f in glob.iglob(dir_path + '/*/' + file_name, recursive=True) if
                 os.path.isfile(f)]
    return len(file_list) > 0


def get_current_date():
    return str(datetime.date.today())


def get_current_date_and_time():
    return datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')


def _create_dir(file_path):
    dir_path = os.path.dirname(file_path)
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def _save_file(content, file_path):
    file_type = "w" if isinstance(content, str) else "wb"
    with open(file_path, file_type) as f:
        f.write(content)


def save_file(layer, content, entity, timestamp, file_name):
    file_path = os.path.join(LAYER_DIR[layer], DATA_SOURCE_NAME, entity, timestamp, file_name)
    _create_dir(file_path)
    _save_file(content, file_path)


def save_raw_file(content, entity, file_name):
    timestamp = get_current_date()
    save_file(RAW_LAYER, content, entity, timestamp, file_name)


def save_temp_df(df: pd.DataFrame, job_id: str, file_name: str):
    temp_dir = os.path.join(TEMP_DIR, job_id)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    # noinspection PyTypeChecker
    df.to_csv(os.path.join(temp_dir, file_name), index=False)


def load_temp_df(job_id: str, file_name: str):
    return pd.read_csv(os.path.join(TEMP_DIR, job_id, file_name))

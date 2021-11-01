"""
This module will store the files in the following structure
- root
  - <layer>
    - <data_source>
      - <entity>
        - <timestamp>
          - <file.extension>
"""
import datetime
import glob
import os
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import ArrowInvalid

from common.entity import Entity
from common.env_variables import DATA_SOURCE_NAME, RAW_DIR, CLEANSED_DIR, TEMP_DIR

RAW_LAYER = 'raw'
CLEANSED_LAYER = 'cleansed'
CURATED_LAYER = 'curated'
TEMP_LAYER = 'temp'

LAYERS = [RAW_LAYER, CLEANSED_LAYER, CURATED_LAYER, TEMP_LAYER]

LAYER_DIR = {
    RAW_LAYER: RAW_DIR,
    CLEANSED_LAYER: CLEANSED_DIR,
    TEMP_LAYER: TEMP_DIR,
}

DOWNLOADED_JOB_DESCRIPTIONS_CSV = '11_downloaded_job_descriptions.csv'
SITEMAP_URLS_CSV = '12_sitemap_urls.csv'
JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV = '13_job_descriptions_to_download.csv'
PARSED_JOB_DESCRIPTIONS_CSV = '21_parsed_job_descriptions.csv'
JOB_DESCRIPTIONS_TO_PARSE_CSV = '22_job_descriptions_to_parse.csv'
DOWNLOADED_SITEMAPS_CSV = '31_downloaded_sitemaps.csv'
PARSED_SITEMAPS_CSV = '32_parsed_sitemaps.csv'
SITEMAPS_TO_PARSE_CSV = '33_sitemaps_to_parse.csv'


def list_raw_files(data_source, entity: Entity):
    dir_path = os.path.join(RAW_DIR, data_source, entity.name)
    file_list = [{
        'timestamp': f.split('/')[-2],
        'file_name': f.split('/')[-1],
    } for f in glob.iglob(dir_path + '/*/*', recursive=True) if os.path.isfile(f)]
    return file_list


def get_current_date():
    return str(datetime.date.today())


def get_run_id():
    return datetime.datetime.today().strftime('%Y/%m/%d/%H-%M-%S')


def _create_dir(file_path):
    dir_path = os.path.dirname(file_path)
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def _save_file(content, file_path):
    file_type = "w" if isinstance(content, str) else "wb"
    with open(file_path, file_type) as f:
        f.write(content)


def save_file(layer, content, entity: Entity, timestamp, file_name):
    file_path = os.path.join(LAYER_DIR[layer], DATA_SOURCE_NAME, entity.name, timestamp, file_name)
    _create_dir(file_path)
    _save_file(content, file_path)


def save_raw_file(content, entity: Entity, file_name):
    timestamp = get_current_date()
    save_file(RAW_LAYER, content, entity, timestamp, file_name)


def load_raw_file(entity: Entity, timestamp, file_name):
    file_path = os.path.join(LAYER_DIR[RAW_LAYER], DATA_SOURCE_NAME, entity.name, timestamp, file_name)
    with open(file_path, 'r') as f:
        content = f.read()
    return content


def save_temp_df(df: pd.DataFrame, run_id: str, file_name: str):
    temp_dir = os.path.join(TEMP_DIR, run_id)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    # noinspection PyTypeChecker
    df.to_csv(os.path.join(temp_dir, file_name), index=False)


def load_temp_df(run_id: str, file_name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(TEMP_DIR, run_id, file_name))


def save_cleansed_df(df: pd.DataFrame, entity: Entity):
    # noinspection PyArgumentList
    table: pa.Table = pa.Table.from_pandas(df, preserve_index=False)
    root_path = os.path.join(LAYER_DIR[CLEANSED_LAYER], DATA_SOURCE_NAME, entity.name)
    pq.write_to_dataset(table,
                        root_path,
                        partition_cols=['year', 'moth', 'day'],
                        use_legacy_dataset=False)


def load_cleansed_df(entity: Entity, columns=None, filters=None) -> pd.DataFrame:
    # noinspection PyArgumentList
    root_path = os.path.join(LAYER_DIR[CLEANSED_LAYER], DATA_SOURCE_NAME, entity.name)
    try:
        table = pq.read_table(root_path, columns, filters=filters, use_legacy_dataset=False)
        return table.to_pandas()
    except (FileNotFoundError, ArrowInvalid):
        return pd.DataFrame(columns=columns)

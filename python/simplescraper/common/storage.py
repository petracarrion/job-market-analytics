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
from azure.storage.blob import BlockBlobService
from dateutil import parser
from pyarrow import ArrowInvalid

from common.entity import Entity
from common.env_variables import DATA_SOURCE_NAME, RAW_DIR, CLEANSED_DIR, TEMP_DIR, AZURE_STORAGE_CONNECTION_STRING, \
    AZURE_STORAGE_CONTAINER_NAME, DATA_DIR, UPLOAD_TO_AZURE
from common.logging import logger

RUN_TIMESTAMP_FORMAT = '%Y/%m/%d/%H-%M-%S'

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
PARSED_SITEMAP_DATES_CSV = '32_parsed_sitemap_dates.csv'
SITEMAPS_TO_PARSE_CSV = '33_sitemaps_to_parse.csv'


def list_raw_files(data_source, entity: Entity):
    dir_path = os.path.join(RAW_DIR, data_source, entity.name)
    file_list = [{
        'run_timestamp': '/'.join(f.split('/')[-5:-1]),
        'file_name': f.split('/')[-1],
    } for f in glob.iglob(dir_path + '/**/*', recursive=True) if os.path.isfile(f) and 'latest' not in f]
    return file_list


def get_run_timestamp(ts=None):
    if ts is not None:
        run_timestamp = parser.parse(ts).strftime(RUN_TIMESTAMP_FORMAT)
    else:
        run_timestamp = datetime.datetime.today().strftime(RUN_TIMESTAMP_FORMAT)
    return run_timestamp


def create_dir(file_path):
    dir_path = os.path.dirname(file_path)
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def save_local_file(content, file_path):
    create_dir(file_path)
    file_type = "w" if isinstance(content, str) else "wb"
    with open(file_path, file_type) as f:
        f.write(content)


def save_remote_file(content, blob_name):
    logger.debug(f'save_remote_file start: {blob_name}')
    blob_service_client = BlockBlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)
    if isinstance(content, str):
        blob_service_client.create_blob_from_text(AZURE_STORAGE_CONTAINER_NAME, blob_name, content)
    else:
        blob_service_client.create_blob_from_bytes(AZURE_STORAGE_CONTAINER_NAME, blob_name, content)
    logger.success(f'save_remote_file end:   {blob_name}')


def save_raw_file(content, entity: Entity, run_timestamp: str, file_name):
    blob_name = os.path.join(RAW_LAYER, DATA_SOURCE_NAME, entity.name, run_timestamp, file_name)
    file_path = os.path.join(DATA_DIR, blob_name)
    save_local_file(content, file_path)
    if UPLOAD_TO_AZURE:
        save_remote_file(content, blob_name)


def load_raw_file(entity: Entity, run_timestamp, file_name):
    file_path = os.path.join(LAYER_DIR[RAW_LAYER], DATA_SOURCE_NAME, entity.name, run_timestamp, file_name)
    with open(file_path, 'r') as f:
        content = f.read()
    return content


def save_temp_df(df: pd.DataFrame, run_timestamp: str, file_name: str):
    temp_dir = os.path.join(TEMP_DIR, run_timestamp)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    # noinspection PyTypeChecker
    df.to_csv(os.path.join(temp_dir, file_name), index=False)


def load_temp_df(run_timestamp: str, file_name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(TEMP_DIR, run_timestamp, file_name))


def list_cleansed_files(entity: Entity, relative_paths=True):
    dir_path = os.path.join(CLEANSED_DIR, DATA_SOURCE_NAME, entity.name)
    file_list = [f for f in glob.iglob(dir_path + '/**/*.parquet', recursive=True) if os.path.isfile(f)]
    if relative_paths:
        file_list = [file_path.replace(dir_path + '/', '') for file_path in file_list]
    return file_list


def save_cleansed_df(df: pd.DataFrame, entity: Entity):
    # noinspection PyArgumentList
    table: pa.Table = pa.Table.from_pandas(df, preserve_index=False)
    root_path = os.path.join(LAYER_DIR[CLEANSED_LAYER], DATA_SOURCE_NAME, entity.name)
    pq.write_to_dataset(table,
                        root_path,
                        partition_cols=['year', 'month', 'day'],
                        use_legacy_dataset=False)


def load_cleansed_df(entity: Entity, columns=None, filters=None) -> pd.DataFrame:
    # noinspection PyArgumentList
    root_path = os.path.join(LAYER_DIR[CLEANSED_LAYER], DATA_SOURCE_NAME, entity.name)
    try:
        table = pq.read_table(root_path, columns, filters=filters, use_legacy_dataset=False)
        return table.to_pandas()
    except (FileNotFoundError, ArrowInvalid):
        return pd.DataFrame(columns=columns)

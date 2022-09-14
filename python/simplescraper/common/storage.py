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
from dateutil import parser
from pyarrow import ArrowInvalid

from common.entity import Entity
from common.env_variables import DATA_SOURCE_NAME, RAW_DIR, CLEANSED_DIR, TEMP_DIR, AZURE_STORAGE_CONNECTION_STRING, \
    AZURE_STORAGE_CONTAINER_NAME, DATA_DIR, UPLOAD_TO_AZURE, BACKUP_DIR, CURATED_DIR
from common.logging import logger

LOAD_TIMESTAMP_FORMAT = '%Y/%m/%d/%H-%M-%S'
LOAD_DATE_FORMAT = '%Y/%m/%d'

RAW_LAYER = 'raw'
CLEANSED_LAYER = 'cleansed'
CURATED_LAYER = 'curated'
TEMP_LAYER = 'temp'

LAYERS = [RAW_LAYER, CLEANSED_LAYER, CURATED_LAYER, TEMP_LAYER]

LAYER_DIR = {
    RAW_LAYER: RAW_DIR,
    CLEANSED_LAYER: CLEANSED_DIR,
    CURATED_LAYER: CURATED_DIR,
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


def list_raw_files(data_source, entity: Entity, load_date=None):
    dir_path = os.path.join(RAW_DIR, data_source, entity.name)
    if load_date:
        dir_path = os.path.join(dir_path, load_date)
    file_list = [{
        'load_timestamp': '/'.join(f.split('/')[-5:-1]),
        'file_name': f.split('/')[-1],
    } for f in glob.iglob(dir_path + '/**/*', recursive=True) if os.path.isfile(f) and 'latest' not in f]
    return file_list


def list_raw_days(data_source, entity: Entity):
    dir_path = os.path.join(RAW_DIR, data_source, entity.name)
    file_list = [{
        'date': ''.join(f.split('/')[-3:]),
    } for f in glob.iglob(dir_path + '/*/*/*', recursive=True) if os.path.isdir(f) and 'latest' not in f]
    return file_list


def list_backup_days(data_source, entity: Entity):
    dir_path = os.path.join(BACKUP_DIR, data_source, entity.name)
    file_list = [{
        'date': f.split('.')[-3],
    } for f in glob.iglob(dir_path + '/**/*', recursive=True) if os.path.isfile(f)]
    return file_list


def get_load_timestamp(ts=None):
    if ts is None:
        load_timestamp = datetime.datetime.today().strftime(LOAD_TIMESTAMP_FORMAT)
    else:
        load_timestamp = parser.parse(ts).strftime(LOAD_TIMESTAMP_FORMAT)
    return load_timestamp


def get_load_date(ds=None):
    if ds is None:
        load_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime(LOAD_DATE_FORMAT)
    else:
        load_date = parser.parse(ds).strftime(LOAD_DATE_FORMAT)
    return load_date


def get_filters_from_load_date(load_date: str):
    year, month, day = load_date.split('/', 2)
    filters = [
        ('year', '=', int(year)),
        ('month', '=', int(month)),
        ('day', '=', int(day)),
    ]
    return filters


def create_dir(file_path):
    dir_path = os.path.dirname(file_path)
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def save_local_file(content, file_path):
    create_dir(file_path)
    file_type = "w" if isinstance(content, str) else "wb"
    with open(file_path, file_type) as f:
        f.write(content)


def save_remote_file(content, blob_name):
    from azure.storage.blob import BlockBlobService
    logger.debug(f'save_remote_file start: {blob_name}')
    blob_service_client = BlockBlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)
    if isinstance(content, str):
        blob_service_client.create_blob_from_text(AZURE_STORAGE_CONTAINER_NAME, blob_name, content)
    else:
        blob_service_client.create_blob_from_bytes(AZURE_STORAGE_CONTAINER_NAME, blob_name, content)
    logger.success(f'save_remote_file end:   {blob_name}')


def save_raw_file(content, entity: Entity, load_timestamp: str, file_name):
    blob_name = os.path.join(RAW_LAYER, DATA_SOURCE_NAME, entity.name, load_timestamp, file_name)
    file_path = os.path.join(DATA_DIR, blob_name)
    save_local_file(content, file_path)
    if UPLOAD_TO_AZURE:
        save_remote_file(content, blob_name)


def load_raw_file(entity: Entity, load_timestamp, file_name):
    file_path = os.path.join(LAYER_DIR[RAW_LAYER], DATA_SOURCE_NAME, entity.name, load_timestamp, file_name)
    with open(file_path, 'r') as f:
        content = f.read()
    return content


def save_temp_df(df: pd.DataFrame, load_timestamp: str, file_name: str):
    temp_dir = os.path.join(TEMP_DIR, load_timestamp)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    # noinspection PyTypeChecker
    df.to_csv(os.path.join(temp_dir, file_name), index=False)


def load_temp_df(load_timestamp: str, file_name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(TEMP_DIR, load_timestamp, file_name))


def list_parquet_files(layer, entity: Entity, relative_paths):
    dir_path = os.path.join(LAYER_DIR[layer], DATA_SOURCE_NAME, entity.name)
    file_list = [f for f in glob.iglob(dir_path + '/**/*.parquet', recursive=True) if os.path.isfile(f)]
    if relative_paths:
        file_list = [file_path.replace(dir_path + '/', '') for file_path in file_list]
    return file_list


def list_cleansed_files(entity: Entity, relative_paths=True):
    return list_parquet_files(CLEANSED_LAYER, entity, relative_paths)


def save_parquet_df(df: pd.DataFrame, layer, entity: Entity):
    # noinspection PyArgumentList
    table: pa.Table = pa.Table.from_pandas(df, preserve_index=False)
    root_path = os.path.join(LAYER_DIR[layer], DATA_SOURCE_NAME, entity.name)
    pq.write_to_dataset(table,
                        root_path,
                        partition_cols=['year', 'month', 'day'],
                        basename_template='part-{i}.parquet',
                        existing_data_behavior='delete_matching',
                        use_legacy_dataset=False)


def save_cleansed_df(df: pd.DataFrame, entity: Entity):
    save_parquet_df(df, CLEANSED_LAYER, entity)


def save_curated_df(df: pd.DataFrame, entity: Entity):
    save_parquet_df(df, CURATED_LAYER, entity)


def load_parquet_df(layer, entity: Entity, columns, filters) -> pd.DataFrame:
    # noinspection PyArgumentList
    root_path = os.path.join(LAYER_DIR[layer], DATA_SOURCE_NAME, entity.name)
    try:
        table = pq.read_table(root_path, columns=columns, filters=filters, use_legacy_dataset=False)
        return table.to_pandas()
    except (FileNotFoundError, ArrowInvalid):
        return pd.DataFrame(columns=columns)


def load_cleansed_df(entity: Entity, columns=None, filters=None, load_date=None) -> pd.DataFrame:
    if filters is None and load_date is not None:
        filters = get_filters_from_load_date(load_date)
    return load_parquet_df(CLEANSED_LAYER, entity, columns, filters)

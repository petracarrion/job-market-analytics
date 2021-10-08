import os

import pandas as pd

DATA_DIR = os.getenv('DATA_DIR', os.path.expanduser('~/job-market-analytics/data'))
RAW_DIR = os.getenv('RAW_DIR', os.path.join(DATA_DIR, 'raw'))
JOB_RESULTS_DIR = os.getenv('JOB_RESULTS_DIR', os.path.join(DATA_DIR, 'results/latest'))

JOB_DIR_NAME = 'www.stepstone.de'

DOWNLOADED_URLS_CSV = '01_downloaded_urls.csv'
SITEMAP_URLS_CSV = '02_sitemap_urls.csv'
URLS_TO_DOWNLOAD_CSV = '03_urls_to_download.csv'


def list_raw_files(dir_name):
    dir_path = os.path.join(RAW_DIR, dir_name)
    return [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]


def save_result_csv(df: pd.DataFrame, file_name):
    if not os.path.exists(JOB_RESULTS_DIR):
        os.makedirs(JOB_RESULTS_DIR)
    # noinspection PyTypeChecker
    df.to_csv(os.path.join(JOB_RESULTS_DIR, file_name), index=False)

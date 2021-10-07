import os

import pandas as pd

from utils.save_results import DOWNLOADED_URLS_CSV, save_csv

PREFIX = 'https://www.stepstone.de/'

DATA_DIR = os.getenv('DATA_DIR', os.path.expanduser('~/job-market-analytics/data'))
RAW_DIR = os.getenv('RAW_DIR', os.path.join(DATA_DIR, 'raw'))
JOB_RESULTS_DIR = os.getenv('JOB_RESULTS_DIR', os.path.join(DATA_DIR, 'results/latest'))

DIR_PATH = os.path.join(RAW_DIR, 'www.stepstone.de')

urls = [PREFIX + f for f in os.listdir(DIR_PATH) if os.path.isfile(os.path.join(DIR_PATH, f))]

df = pd.DataFrame(urls, columns=['job_url'])
save_csv(df, os.path.join(JOB_RESULTS_DIR, DOWNLOADED_URLS_CSV))

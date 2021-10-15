import os

from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv('DATA_DIR', os.path.expanduser('~/job-market-analytics/data'))

DATA_SOURCE_NAME = os.getenv('DATA_SOURCE_NAME', 'datasource')

print(DATA_SOURCE_NAME)
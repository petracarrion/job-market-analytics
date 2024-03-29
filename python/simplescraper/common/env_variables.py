import os

from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv('DATA_DIR')
RAW_DIR = os.getenv('RAW_DIR')
CLEANSED_DIR = os.getenv('CLEANSED_DIR')
CURATED_DIR = os.getenv('CURATED_DIR')
DUCKDB_DWH_FILE = os.getenv('DUCKDB_DWH_FILE')
TEMP_DIR = os.getenv('TEMP_DIR')
BACKUP_DIR = os.getenv('BACKUP_DIR')
SOURCE_DIR = os.getenv('SOURCE_DIR')

DATA_SOURCE_NAME = os.getenv('DATA_SOURCE_NAME')
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')

SEMAPHORE_COUNT: int = int(os.getenv('SEMAPHORE_COUNT'))
MAX_CHUNK_SIZE: int = int(os.getenv('MAX_CHUNK_SIZE'))
MIN_TO_DOWNLOAD: int = int(os.getenv('MIN_TO_DOWNLOAD'))
MAX_TO_DOWNLOAD: int = int(os.getenv('MAX_TO_DOWNLOAD'))
ONLINE_EXPIRATION_IN_DAYS: int = int(os.getenv('ONLINE_EXPIRATION_IN_DAYS'))

LATEST_LOAD_TIMESTAMP = os.getenv('LATEST_LOAD_TIMESTAMP')

RUN_HEADLESS = os.getenv('RUN_HEADLESS') == 'True'

UPLOAD_TO_AZURE = os.getenv('UPLOAD_TO_AZURE') == 'True'

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_STORAGE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_NAME')

import os

from dotenv import load_dotenv

load_dotenv()

RAW_DIR = os.getenv('RAW_DIR')
CLEANSED_DIR = os.getenv('CLEANSED_DIR')
TEMP_DIR = os.getenv('TEMP_DIR')

DATA_SOURCE_NAME = os.getenv('DATA_SOURCE_NAME')
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')

SEMAPHORE_COUNT: int = int(os.getenv('SEMAPHORE_COUNT'))
MAX_CHUNK_SIZE: int = int(os.getenv('MAX_CHUNK_SIZE'))

LATEST_RUN_ID = os.getenv('LATEST_RUN_ID')

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

RUN_HEADLESS = os.getenv('RUN_HEADLESS') == 'True'

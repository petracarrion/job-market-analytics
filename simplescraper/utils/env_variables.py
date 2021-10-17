import os

from dotenv import load_dotenv

load_dotenv()

RAW_DIR = os.getenv('RAW_DIR')
TEMP_DIR = os.getenv('TEMP_DIR')

DATA_SOURCE_NAME = os.getenv('DATA_SOURCE_NAME')
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')

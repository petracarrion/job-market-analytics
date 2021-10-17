import os

from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv('DATA_DIR')

DATA_SOURCE_NAME = os.getenv('DATA_SOURCE_NAME')
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')

TEMP_DIR = os.getenv('TEMP_DIR')

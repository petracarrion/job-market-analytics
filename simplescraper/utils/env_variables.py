import os

from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv('DATA_DIR')

DATA_SOURCE_NAME = os.getenv('DATA_SOURCE_NAME')
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')

LOG_DIR = os.getenv('LOG_DIR')

print(DATA_SOURCE_NAME)
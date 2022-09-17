import os.path

from loguru import logger

from common.env_variables import TEMP_DIR


def configure_logger(load_timestamp):
    logger.add(os.path.join(TEMP_DIR, load_timestamp, f'00_logs.log'))


logger = logger

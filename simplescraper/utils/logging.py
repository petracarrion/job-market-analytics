import os.path

from loguru import logger

from utils.env_variables import LOG_DIR


def get_logger():
    logger.add(os.path.join(LOG_DIR, 'job-market-analysis.{time}.log'))
    return logger

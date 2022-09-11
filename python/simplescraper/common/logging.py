import os.path

from loguru import logger

from common.env_variables import TEMP_DIR


def configure_logger(run_timestamp, task_name):
    logger.add(os.path.join(TEMP_DIR, run_timestamp, f'00_{task_name}.log'))
    return logger


logger = logger

import os.path

from loguru import logger

from common.env_variables import TEMP_DIR


def configure_logger(job_id):
    logger.add(os.path.join(TEMP_DIR, job_id, '00_job_run.log'))


def get_logger():
    return logger

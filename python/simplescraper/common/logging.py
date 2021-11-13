import os.path

from loguru import logger

from common.env_variables import TEMP_DIR


def configure_logger(run_id):
    logger.add(os.path.join(TEMP_DIR, run_id, '00_pipeline_run.log'))


def get_logger():
    return logger

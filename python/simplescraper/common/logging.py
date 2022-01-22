import os.path

from loguru import logger

from common.env_variables import TEMP_DIR


def configure_logger(run_timestamp):
    logger.add(os.path.join(TEMP_DIR, run_timestamp, '00_pipeline_run.log'))


logger = logger

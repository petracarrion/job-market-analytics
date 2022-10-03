import datetime
import os
import shutil
import sys

from common.entity import RAW_ENTITIES
from common.env_variables import RAW_DIR, DATA_SOURCE_NAME
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_load_date, LOAD_DATE_FORMAT

SEVEN_MONTHS_IN_DAYS = 7 * 30


def prune_old_raw(load_timestamp, load_date):
    configure_logger(load_timestamp)
    logger.info(f'Start prune_old_raw: {load_date}')
    date_to_remove = datetime.datetime.strptime(load_date, LOAD_DATE_FORMAT).date()
    date_to_remove = date_to_remove - datetime.timedelta(days=SEVEN_MONTHS_IN_DAYS)
    date_to_remove = date_to_remove.strftime(LOAD_DATE_FORMAT)
    year, month, day = date_to_remove.split('/', 2)
    for entity in RAW_ENTITIES:
        folder_to_remove = f'{RAW_DIR}/{DATA_SOURCE_NAME}/{entity}/{year}/{month}/{day}'
        if os.path.exists(folder_to_remove) and os.path.isdir(folder_to_remove):
            logger.success(f'Removing {folder_to_remove}')
            shutil.rmtree(folder_to_remove)
        else:
            logger.warning(f'No folder to remove on {folder_to_remove}')

    logger.info(f'End   prune_old_raw: {load_date}')


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    prune_old_raw(_load_timestamp, _load_date)

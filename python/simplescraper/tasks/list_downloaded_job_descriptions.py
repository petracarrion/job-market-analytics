import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import LATEST_RUN_TIMESTAMP
from common.logging import logger
from common.storage import DOWNLOADED_JOB_DESCRIPTIONS_CSV, DATA_SOURCE_NAME, save_temp_df, list_raw_files


def list_downloaded_job_descriptions(run_timestamp) -> pd.DataFrame:
    logger.info('list_downloaded_job_descriptions start')
    files = list_raw_files(DATA_SOURCE_NAME, JOB_DESCRIPTION)
    df = pd.DataFrame(files)
    df['id'] = df['file_name'].str.split('.', expand=True)[0]
    save_temp_df(df, run_timestamp, DOWNLOADED_JOB_DESCRIPTIONS_CSV)
    logger.info('list_downloaded_job_descriptions end')
    return df


if __name__ == "__main__":
    list_downloaded_job_descriptions(LATEST_RUN_TIMESTAMP)

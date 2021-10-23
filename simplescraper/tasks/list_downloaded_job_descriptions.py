import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import DATA_SOURCE_URL
from common.storage import DOWNLOADED_URLS_CSV, DATA_SOURCE_NAME, save_temp_df, list_raw_files, \
    get_current_date_and_time


def list_downloaded_job_descriptions(job_id):
    files = list_raw_files(DATA_SOURCE_NAME, JOB_DESCRIPTION)
    df = pd.DataFrame(files)
    df['url'] = DATA_SOURCE_URL + df['file_name']
    save_temp_df(df, job_id, DOWNLOADED_URLS_CSV)
    return df


if __name__ == "__main__":
    list_downloaded_job_descriptions(get_current_date_and_time())

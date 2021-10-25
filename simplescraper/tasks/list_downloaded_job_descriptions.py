import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import DATA_SOURCE_URL, LATEST_RUN_ID
from common.storage import DOWNLOADED_JOB_DESCRIPTIONS_CSV, DATA_SOURCE_NAME, save_temp_df, list_raw_files


def list_downloaded_job_descriptions(run_id) -> pd.DataFrame:
    files = list_raw_files(DATA_SOURCE_NAME, JOB_DESCRIPTION)
    df = pd.DataFrame(files)
    df['url'] = DATA_SOURCE_URL + df['file_name']
    save_temp_df(df, run_id, DOWNLOADED_JOB_DESCRIPTIONS_CSV)
    return df


if __name__ == "__main__":
    list_downloaded_job_descriptions(LATEST_RUN_ID)

import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import LATEST_RUN_ID
from common.storage import load_cleansed_df, save_temp_df, PARSED_JOB_DESCRIPTIONS_CSV, get_run_id


def list_parsed_job_descriptions(run_id) -> pd.DataFrame:
    df = load_cleansed_df(JOB_DESCRIPTION, columns=['timestamp', 'file_name', 'url'])
    save_temp_df(df, run_id, PARSED_JOB_DESCRIPTIONS_CSV)
    return df


if __name__ == "__main__":
    list_parsed_job_descriptions(LATEST_RUN_ID)

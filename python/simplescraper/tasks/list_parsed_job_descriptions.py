import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_cleansed_df, save_temp_df, PARSED_JOB_DESCRIPTIONS_CSV


def list_parsed_job_descriptions(run_timestamp) -> pd.DataFrame:
    df = load_cleansed_df(JOB_DESCRIPTION, columns=['run_timestamp', 'file_name', 'id'])
    save_temp_df(df, run_timestamp, PARSED_JOB_DESCRIPTIONS_CSV)
    return df


if __name__ == "__main__":
    list_parsed_job_descriptions(LATEST_RUN_TIMESTAMP)

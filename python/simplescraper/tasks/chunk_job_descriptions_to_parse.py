import pandas as pd

from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_temp_df, JOB_DESCRIPTIONS_TO_PARSE_CSV


def number_of_rows(df: pd.DataFrame):
    return df.shape[0]


def chunk_job_descriptions_to_parse(run_timestamp, df_to_parse):
    dfs = [x for _, x in df_to_parse.groupby('run_timestamp')]
    dfs = sorted(dfs, key=number_of_rows)
    return dfs


if __name__ == "__main__":
    chunk_job_descriptions_to_parse(
        LATEST_RUN_TIMESTAMP,
        load_temp_df(LATEST_RUN_TIMESTAMP, JOB_DESCRIPTIONS_TO_PARSE_CSV)
    )

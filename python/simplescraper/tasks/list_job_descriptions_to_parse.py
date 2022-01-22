import pandas as pd

from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_temp_df, DOWNLOADED_JOB_DESCRIPTIONS_CSV, PARSED_JOB_DESCRIPTIONS_CSV, save_temp_df, \
    JOB_DESCRIPTIONS_TO_PARSE_CSV


def list_job_descriptions_to_parse(run_timestamp, df_downloaded: pd.DataFrame, df_parsed: pd.DataFrame) -> pd.DataFrame:
    df_downloaded = df_downloaded.drop_duplicates()
    df = pd.concat([df_downloaded, df_parsed, df_parsed]).drop_duplicates(keep=False)
    save_temp_df(df, run_timestamp, JOB_DESCRIPTIONS_TO_PARSE_CSV)
    return df


if __name__ == "__main__":
    list_job_descriptions_to_parse(
        LATEST_RUN_TIMESTAMP,
        load_temp_df(LATEST_RUN_TIMESTAMP, DOWNLOADED_JOB_DESCRIPTIONS_CSV),
        load_temp_df(LATEST_RUN_TIMESTAMP, PARSED_JOB_DESCRIPTIONS_CSV)
    )

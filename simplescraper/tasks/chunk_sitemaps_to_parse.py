import pandas as pd

from common.env_variables import LATEST_RUN_ID
from common.storage import load_temp_df, JOB_DESCRIPTIONS_TO_PARSE_CSV, SITEMAPS_TO_PARSE_CSV


def chunk_sitemaps_to_parse(run_id, df_to_parse):
    dfs = [x for _, x in df_to_parse.groupby('timestamp')]
    return dfs


if __name__ == "__main__":
    chunk_sitemaps_to_parse(
        LATEST_RUN_ID,
        load_temp_df(LATEST_RUN_ID, SITEMAPS_TO_PARSE_CSV)
    )

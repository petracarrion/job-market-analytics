from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_temp_df, SITEMAPS_TO_PARSE_CSV


def chunk_sitemaps_to_parse(run_timestamp, df_to_parse):
    dfs = [x for _, x in df_to_parse.groupby('timestamp')]
    return dfs


if __name__ == "__main__":
    chunk_sitemaps_to_parse(
        LATEST_RUN_TIMESTAMP,
        load_temp_df(LATEST_RUN_TIMESTAMP, SITEMAPS_TO_PARSE_CSV)
    )

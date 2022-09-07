import pandas as pd

from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_temp_df, DOWNLOADED_SITEMAPS_CSV, PARSED_SITEMAP_DATES_CSV, save_temp_df, \
    SITEMAPS_TO_PARSE_CSV


def list_sitemaps_to_parse(run_timestamp, df_downloaded: pd.DataFrame, df_parsed: pd.DataFrame) -> pd.DataFrame:
    df_downloaded = df_downloaded.drop_duplicates()
    parsed_dates = df_parsed['run_timestamp'].to_list()
    df = df_downloaded[~df_downloaded['run_timestamp'].isin(parsed_dates)]
    save_temp_df(df, run_timestamp, SITEMAPS_TO_PARSE_CSV)
    return df


if __name__ == "__main__":
    list_sitemaps_to_parse(
        LATEST_RUN_TIMESTAMP,
        load_temp_df(LATEST_RUN_TIMESTAMP, DOWNLOADED_SITEMAPS_CSV),
        load_temp_df(LATEST_RUN_TIMESTAMP, PARSED_SITEMAP_DATES_CSV)
    )

import pandas as pd

from common.env_variables import LATEST_RUN_ID
from common.storage import load_temp_df, DOWNLOADED_SITEMAPS_CSV, PARSED_SITEMAPS_CSV, save_temp_df, \
    SITEMAPS_TO_PARSE_CSV


def list_sitemaps_to_parse(run_id, df_downloaded: pd.DataFrame, df_parsed: pd.DataFrame) -> pd.DataFrame:
    df_downloaded = df_downloaded.drop_duplicates()
    df = pd.concat([df_downloaded, df_parsed, df_parsed]).drop_duplicates(keep=False)
    save_temp_df(df, run_id, SITEMAPS_TO_PARSE_CSV)
    return df


if __name__ == "__main__":
    list_sitemaps_to_parse(
        LATEST_RUN_ID,
        load_temp_df(LATEST_RUN_ID, DOWNLOADED_SITEMAPS_CSV),
        load_temp_df(LATEST_RUN_ID, PARSED_SITEMAPS_CSV)
    )

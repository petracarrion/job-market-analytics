import pandas as pd

from common.entity import SITEMAP
from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_cleansed_df, save_temp_df, PARSED_SITEMAPS_CSV


def list_parsed_sitemaps(run_timestamp) -> pd.DataFrame:
    df = load_cleansed_df(SITEMAP, columns=['run_timestamp', 'file_name'])
    df = df.drop_duplicates()
    save_temp_df(df, run_timestamp, PARSED_SITEMAPS_CSV)
    return df


if __name__ == "__main__":
    list_parsed_sitemaps(LATEST_RUN_TIMESTAMP)

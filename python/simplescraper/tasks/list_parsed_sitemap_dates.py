import pandas as pd

from common.entity import SITEMAP
from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import load_cleansed_df, save_temp_df, PARSED_SITEMAP_DATES_CSV


def list_parsed_sitemap_dates(run_timestamp) -> pd.DataFrame:
    df = load_cleansed_df(SITEMAP, columns=['ingestion_date'])
    df = df.drop_duplicates()
    save_temp_df(df, run_timestamp, PARSED_SITEMAP_DATES_CSV)
    return df


if __name__ == "__main__":
    list_parsed_sitemap_dates(LATEST_RUN_TIMESTAMP)

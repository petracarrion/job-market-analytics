import pandas as pd

from common.entity import SITEMAP
from common.env_variables import LATEST_RUN_TIMESTAMP
from common.storage import DATA_SOURCE_NAME, save_temp_df, list_raw_files, DOWNLOADED_SITEMAPS_CSV, get_target_date


def list_downloaded_sitemaps(run_timestamp, target_date=None) -> pd.DataFrame:
    files = list_raw_files(DATA_SOURCE_NAME, SITEMAP, target_date)
    df = pd.DataFrame(files)
    df = df[df['file_name'] != 'sitemapindex.xml']
    if target_date is None:
        save_temp_df(df, run_timestamp, DOWNLOADED_SITEMAPS_CSV)
    return df


if __name__ == "__main__":
    list_downloaded_sitemaps(LATEST_RUN_TIMESTAMP, get_target_date())

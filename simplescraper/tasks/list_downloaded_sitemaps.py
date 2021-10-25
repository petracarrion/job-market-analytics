import pandas as pd

from common.entity import SITEMAP
from common.env_variables import LATEST_RUN_ID
from common.storage import DATA_SOURCE_NAME, save_temp_df, list_raw_files, DOWNLOADED_SITEMAPS_CSV


def list_downloaded_sitemaps(run_id) -> pd.DataFrame:
    files = list_raw_files(DATA_SOURCE_NAME, SITEMAP)
    df = pd.DataFrame(files)
    save_temp_df(df, run_id, DOWNLOADED_SITEMAPS_CSV)
    return df


if __name__ == "__main__":
    list_downloaded_sitemaps(LATEST_RUN_ID)

import pandas as pd

from utils.env_variables import DATA_SOURCE_URL
from utils.storage import DOWNLOADED_URLS_CSV, DATA_SOURCE_NAME, save_temp_df, list_raw_files


def list_downloaded_urls():
    file_names = list_raw_files(DATA_SOURCE_NAME, 'job_description')
    urls = [DATA_SOURCE_URL + file_name for file_name in file_names]
    df = pd.DataFrame(urls, columns=['job_url'])
    save_temp_df(df, DOWNLOADED_URLS_CSV)


if __name__ == "__main__":
    list_downloaded_urls()

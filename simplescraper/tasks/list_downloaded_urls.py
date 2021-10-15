import pandas as pd

from utils.storage import DOWNLOADED_URLS_CSV, DATA_SOURCE, save_temp_df, list_raw_files

URL_PREFIX = 'https://www.stepstone.de/'


def list_downloaded_urls():
    file_names = list_raw_files(DATA_SOURCE, 'job_description')
    urls = [URL_PREFIX + file_name for file_name in file_names]
    df = pd.DataFrame(urls, columns=['job_url'])
    save_temp_df(df, DOWNLOADED_URLS_CSV)


if __name__ == "__main__":
    list_downloaded_urls()

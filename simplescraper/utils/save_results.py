import os

import pandas as pd

DOWNLOADED_URLS_CSV = '01_downloaded_urls.csv'


def save_csv(df: pd.DataFrame, file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv(file_path, index=False)

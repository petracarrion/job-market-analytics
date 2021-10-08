import pandas as pd

from utils.storage import DOWNLOADED_URLS_CSV, JOB_DIR_NAME, save_result_csv, list_raw_files

URL_PREFIX = 'https://www.stepstone.de/'


def main():
    file_names = list_raw_files(JOB_DIR_NAME)
    urls = [URL_PREFIX + file_name for file_name in file_names]
    df = pd.DataFrame(urls, columns=['job_url'])
    save_result_csv(df, DOWNLOADED_URLS_CSV)


if __name__ == "__main__":
    main()

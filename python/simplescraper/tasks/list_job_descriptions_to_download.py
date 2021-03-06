import pandas as pd

from common.env_variables import LATEST_RUN_TIMESTAMP
from common.logging import logger
from common.storage import load_temp_df, DOWNLOADED_JOB_DESCRIPTIONS_CSV, SITEMAP_URLS_CSV, save_temp_df, \
    JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV


def list_job_descriptions_to_download(run_timestamp, df_sitemap_urls=None, df_downloaded=None):
    logger.info('list_job_descriptions_to_download: start')

    df_sitemap_urls = df_sitemap_urls or load_temp_df(run_timestamp, SITEMAP_URLS_CSV)
    df_downloaded = df_downloaded or load_temp_df(run_timestamp, DOWNLOADED_JOB_DESCRIPTIONS_CSV)

    df_downloaded = df_downloaded[['id']]
    df_downloaded = df_downloaded.drop_duplicates()
    df = df_sitemap_urls[['id']]
    df = df.drop_duplicates()
    df = pd.concat([df, df_downloaded, df_downloaded]).drop_duplicates(keep=False)
    df = df.merge(df_sitemap_urls)
    df = df[['url']]
    total_count = df.shape[0]

    save_temp_df(df, run_timestamp, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV)
    logger.success(f'URLs to download: {total_count}')
    logger.info('list_job_descriptions_to_download: end')
    return df


if __name__ == "__main__":
    list_job_descriptions_to_download(LATEST_RUN_TIMESTAMP)

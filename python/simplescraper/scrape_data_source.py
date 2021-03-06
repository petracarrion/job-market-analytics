import os

from common.logging import configure_logger
from common.storage import get_run_timestamp
from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.list_job_descriptions_to_download import list_job_descriptions_to_download


def scrape_data_source(run_timestamp):
    configure_logger(run_timestamp)
    df_downloaded = list_downloaded_job_descriptions(run_timestamp)
    df_sitemap = download_sitemap(run_timestamp)
    df_to_download = list_job_descriptions_to_download(run_timestamp, df_sitemap, df_downloaded)
    download_job_descriptions(run_timestamp, df_to_download)

    os.system('say -v Fiona b')


if __name__ == "__main__":
    scrape_data_source(get_run_timestamp())

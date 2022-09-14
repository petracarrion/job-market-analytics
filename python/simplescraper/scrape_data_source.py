import os

from common.logging import configure_logger
from common.storage import get_load_timestamp
from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.list_job_descriptions_to_download import list_job_descriptions_to_download


def scrape_data_source(load_timestamp):
    configure_logger(load_timestamp, 'scrape_data_source')
    df_downloaded = list_downloaded_job_descriptions(load_timestamp)
    df_sitemap = download_sitemap(load_timestamp)
    df_to_download = list_job_descriptions_to_download(load_timestamp, df_sitemap, df_downloaded)
    download_job_descriptions(load_timestamp, df_to_download)

    os.system('say -v Fiona b')


if __name__ == "__main__":
    scrape_data_source(get_load_timestamp())

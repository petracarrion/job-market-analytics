import os

from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_urls import list_downloaded_urls
from utils.storage import get_current_date_and_time


def scraper_data_source():
    job_id = get_current_date_and_time()
    list_downloaded_urls(job_id)
    download_sitemap(job_id)
    download_job_descriptions(job_id)
    os.system('say -v Zuzana A je to')


if __name__ == "__main__":
    scraper_data_source()

from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_urls import list_downloaded_urls


def simplescraper():
    list_downloaded_urls()
    download_sitemap()
    download_job_descriptions()


if __name__ == "__main__":
    simplescraper()

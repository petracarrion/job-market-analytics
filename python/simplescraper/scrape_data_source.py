from common.logging import configure_logger
from common.storage import get_run_id
from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.list_job_descriptions_to_download import list_job_descriptions_to_download


def scraper_data_source():
    run_id = get_run_id()
    configure_logger(run_id)
    df_downloaded = list_downloaded_job_descriptions(run_id)
    df_sitemap = download_sitemap(run_id)
    df_to_download = list_job_descriptions_to_download(run_id, df_sitemap, df_downloaded)
    download_job_descriptions(run_id, df_to_download)

    # os.system('say -v Zuzana A je to')


if __name__ == "__main__":
    scraper_data_source()

from common.logging import configure_logger
from common.storage import get_job_id
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps


def parse_sitemaps():
    job_id = get_job_id()
    configure_logger(job_id)
    df = list_downloaded_sitemaps(job_id)
    print(df)


if __name__ == "__main__":
    parse_sitemaps()
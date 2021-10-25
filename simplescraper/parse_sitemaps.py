from common.logging import configure_logger
from common.storage import get_run_id
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps


def parse_sitemaps():
    run_id = get_run_id()
    configure_logger(run_id)
    df = list_downloaded_sitemaps(run_id)
    print(df)


if __name__ == "__main__":
    parse_sitemaps()
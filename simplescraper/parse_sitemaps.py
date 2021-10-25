import bs4
from loguru import logger

from common.entity import SITEMAP
from common.logging import configure_logger
from common.storage import get_run_id, load_raw_file
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps

DEBUG = True


def load_and_parse(row):
    timestamp = row['timestamp']
    file_name = row['file_name']
    sitemap_content = load_raw_file(SITEMAP, timestamp, file_name)
    logger.debug(f'Parsing: {timestamp}/{file_name}')
    soup = bs4.BeautifulSoup(sitemap_content, 'lxml')
    urls = [loc.text for loc in soup.findAll('loc')]
    return urls


def parse_sitemaps():
    run_id = get_run_id()
    configure_logger(run_id)
    df = list_downloaded_sitemaps(run_id)
    if DEBUG:
        df = df[df['timestamp'] == '2021-10-19']
    df['url'] = df.apply(load_and_parse, axis=1)
    df = df.explode('url')
    print(df)


if __name__ == "__main__":
    parse_sitemaps()

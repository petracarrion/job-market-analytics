import bs4
from loguru import logger

from common.entity import SITEMAP
from common.logging import configure_logger
from common.storage import get_run_id, load_raw_file, save_cleansed_df
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps
from tasks.list_parsed_sitemaps import list_parsed_sitemaps
from tasks.list_sitemaps_to_parse import list_sitemaps_to_parse

DEBUG = False


def load_and_parse(row):
    timestamp = row['timestamp']
    file_name = row['file_name']
    sitemap_content = load_raw_file(SITEMAP, timestamp, file_name)
    logger.debug(f'Parsing: {timestamp}/{file_name}')
    soup = bs4.BeautifulSoup(sitemap_content, 'lxml')
    urls = [loc.text for loc in soup.findAll('loc')]
    return urls


def extract_job_id(url_column):
    url_split = url_column.str.split('--', expand=True)
    return url_split[2].str.split('-', expand=True)[0]


def parse_sitemaps():
    run_id = get_run_id()
    configure_logger(run_id)
    df_downloaded = list_downloaded_sitemaps(run_id)
    df_parsed = list_parsed_sitemaps(run_id)
    df = list_sitemaps_to_parse(run_id, df_downloaded, df_parsed)
    if DEBUG:
        df = df[df['timestamp'] == '2021-11-01']
    if df.empty:
        logger.info('Nothing to parse')
        return
    df['url'] = df.apply(load_and_parse, axis=1)
    df = df.explode('url')
    df['job_id'] = extract_job_id(df['url'])
    df[['year', 'moth', 'day']] = df['timestamp'].str.split('-', 2, expand=True)

    save_cleansed_df(df, SITEMAP)


if __name__ == "__main__":
    parse_sitemaps()

import sys

import bs4
from loguru import logger

from common.entity import SITEMAP
from common.logging import configure_logger
from common.storage import get_load_timestamp, load_raw_file, save_cleansed_df, get_load_date
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps


def load_and_parse(row):
    load_timestamp = row['load_timestamp']
    file_name = row['file_name']
    sitemap_content = load_raw_file(SITEMAP, load_timestamp, file_name)
    logger.debug(f'Parsing: {load_timestamp}/{file_name}')
    soup = bs4.BeautifulSoup(sitemap_content, 'lxml')
    urls = [loc.text for loc in soup.findAll('loc')]
    return urls


def extract_job_id(url_column):
    url_split = url_column.str.split('--', expand=True)
    return url_split[2].str.split('-', expand=True)[0]


def get_date_from_load_timestamp(load_timestamp):
    year, month, day, time = load_timestamp.split('/')
    return f'{year}-{month}-{day}'


def cleanse_sitemaps(load_timestamp, load_date):
    configure_logger(load_timestamp, 'parse_sitemaps')
    df = list_downloaded_sitemaps(load_timestamp, load_date)
    df[['year', 'month', 'day', 'time']] = df['load_timestamp'].str.split('/', 3, expand=True)
    if df.empty:
        logger.info('Nothing to parse')
        return
    last_time = sorted(df.time.unique().tolist()).pop()
    df = df[df['time'] == last_time]
    df = df.sort_values(by=['load_timestamp', 'file_name'])
    df['url'] = df.apply(load_and_parse, axis=1)
    df = df.explode('url')
    df = df.drop_duplicates(['load_timestamp', 'url'], keep='last')
    df['job_id'] = extract_job_id(df['url'])

    logger.info(f'Saving cleansed: {df["load_timestamp"].iloc[0]}')
    save_cleansed_df(df, SITEMAP)


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    cleanse_sitemaps(_load_timestamp, _load_date)

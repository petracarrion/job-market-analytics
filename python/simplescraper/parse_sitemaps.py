import hashlib

import bs4
from loguru import logger

from common.entity import SITEMAP
from common.logging import configure_logger
from common.storage import get_run_timestamp, load_raw_file, save_cleansed_df
from tasks.chunk_sitemaps_to_parse import chunk_sitemaps_to_parse
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps
from tasks.list_parsed_sitemap_dates import list_parsed_sitemap_dates
from tasks.list_sitemaps_to_parse import list_sitemaps_to_parse

HASHKEY_SEPARATOR = ';'

DEBUG = False


def load_and_parse(row):
    run_timestamp = row['run_timestamp']
    file_name = row['file_name']
    sitemap_content = load_raw_file(SITEMAP, run_timestamp, file_name)
    logger.debug(f'Parsing: {run_timestamp}/{file_name}')
    soup = bs4.BeautifulSoup(sitemap_content, 'lxml')
    urls = [loc.text for loc in soup.findAll('loc')]
    return urls


def extract_job_id(url_column):
    url_split = url_column.str.split('--', expand=True)
    return url_split[2].str.split('-', expand=True)[0]


def get_run_timestamp_date(run_timestamp):
    year, month, day, time = run_timestamp.split('/')
    return f'{year}-{month}-{day}'


def remove_current_date(df, run_timestamp):
    run_timestamp_date = get_run_timestamp_date(run_timestamp)
    df['ingestion_date'] = df['year'] + '-' + df['month'] + '-' + df['day']
    df = df[df['ingestion_date'] != run_timestamp_date]
    return df


def parse_sitemaps(run_timestamp):
    configure_logger(run_timestamp, 'parse_sitemaps')
    df_downloaded = list_downloaded_sitemaps(run_timestamp)
    df_downloaded[['year', 'month', 'day', 'time']] = df_downloaded['run_timestamp'].str.split('/', 3, expand=True)
    df_downloaded = remove_current_date(df_downloaded, run_timestamp)
    df_parsed_sitemap_dates = list_parsed_sitemap_dates(run_timestamp)
    df = list_sitemaps_to_parse(run_timestamp, df_downloaded, df_parsed_sitemap_dates)
    if DEBUG:
        df = df[df['ingestion_date'] == '2022-03-17']
    if df.empty:
        logger.info('Nothing to parse')
        return
    dfs = chunk_sitemaps_to_parse(df)
    for df in dfs:
        last_time = sorted(df.time.unique().tolist()).pop()
        df = df[df['time'] == last_time]
        df = df.sort_values(by=['run_timestamp', 'file_name'])
        df['url'] = df.apply(load_and_parse, axis=1)
        df = df.explode('url')
        df = df.drop_duplicates(['ingestion_date', 'url'], keep='last')
        df['job_id'] = extract_job_id(df['url'])
        df['sitemap_ingestion_hashkey'] = df.apply(
            lambda row: hashlib.md5(
                f'{row["job_id"]}{HASHKEY_SEPARATOR}{row["ingestion_date"]}'.encode('utf-8')).hexdigest(),
            axis=1)

        logger.info(f'Saving cleansed: {df["ingestion_date"].iloc[0]}')
        save_cleansed_df(df, SITEMAP)


if __name__ == "__main__":
    parse_sitemaps(get_run_timestamp())

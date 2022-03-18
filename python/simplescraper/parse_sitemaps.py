import hashlib

import bs4
from loguru import logger

from common.entity import SITEMAP
from common.logging import configure_logger
from common.storage import get_run_timestamp, load_raw_file, save_cleansed_df
from tasks.chunk_sitemaps_to_parse import chunk_sitemaps_to_parse
from tasks.list_downloaded_sitemaps import list_downloaded_sitemaps
from tasks.list_parsed_sitemaps import list_parsed_sitemaps
from tasks.list_sitemaps_to_parse import list_sitemaps_to_parse

HASHKEY_SEPARATOR = ';'

DEBUG = True


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


def parse_sitemaps():
    run_timestamp = get_run_timestamp()
    configure_logger(run_timestamp)
    df_downloaded = list_downloaded_sitemaps(run_timestamp)
    df_parsed = list_parsed_sitemaps(run_timestamp)
    df = list_sitemaps_to_parse(run_timestamp, df_downloaded, df_parsed)
    if DEBUG:
        df = df[df['run_timestamp'] == '2022/03/18/11-00-00']
    if df.empty:
        logger.info('Nothing to parse')
        return
    dfs = chunk_sitemaps_to_parse(df)
    for df in dfs:
        df = df.sort_values(by=['run_timestamp', 'file_name'])
        df['url'] = df.apply(load_and_parse, axis=1)
        df = df.explode('url')
        df[['year', 'month', 'day', 'time']] = df['run_timestamp'].str.split('/', 3, expand=True)
        df['job_id'] = extract_job_id(df['url'])
        df['sitemap_ingestion_hashkey'] = df.apply(
            lambda row: hashlib.md5(
                f'{row["job_id"]}{HASHKEY_SEPARATOR}{row["run_timestamp"]}'.encode('utf-8')).hexdigest(),
            axis=1)

        df = df.convert_dtypes()

        save_cleansed_df(df, SITEMAP)


if __name__ == "__main__":
    parse_sitemaps()

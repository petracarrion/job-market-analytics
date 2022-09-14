import datetime

import pandas as pd
import xmltodict

from common.entity import SITEMAP
from common.env_variables import DATA_SOURCE_URL, LATEST_LOAD_TIMESTAMP
from common.logging import logger, configure_logger
from common.storage import save_temp_df, SITEMAP_URLS_CSV, save_raw_file, LOAD_TIMESTAMP_FORMAT
from common.webclient import get_url_content

SITEMAP_INDEX_XML = f'{DATA_SOURCE_URL}5/sitemaps/de/sitemapindex.xml'

ONE_HOUR = 3600


def check_load_timestamp(load_timestamp):
    parsed_load_timestamp = datetime.datetime.strptime(load_timestamp, LOAD_TIMESTAMP_FORMAT).replace(
        tzinfo=datetime.timezone.utc)
    current_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    time_delta = current_timestamp - parsed_load_timestamp
    if time_delta.seconds > ONE_HOUR:
        raise Exception('The load_timestamp is older than one hour')


def historize_url_content(url, content, load_timestamp):
    file_name = url.split('/')[-1]
    save_raw_file(content, SITEMAP, load_timestamp, file_name)


def get_and_historize_url_content(url, load_timestamp):
    content = get_url_content(url)
    historize_url_content(url, content, load_timestamp)
    return content


def get_listing_urls(load_timestamp):
    web_content = get_and_historize_url_content(SITEMAP_INDEX_XML, load_timestamp)
    web_content = xmltodict.parse(web_content)
    web_content = web_content['sitemapindex']
    web_content = web_content['sitemap']
    listing_urls = []
    for entry in web_content:
        url = entry['loc']
        if 'listings' in url:
            listing_urls.append(url)
    return listing_urls


def get_job_description_urls(web_content):
    web_content = xmltodict.parse(web_content)
    web_content = web_content['urlset']
    url_entries = web_content['url']
    urls = []
    for entry in url_entries:
        url = entry['loc']
        urls.append(url)

    return urls


def get_all_job_description_urls(load_timestamp):
    listing_urls = get_listing_urls(load_timestamp)
    job_description_urls = []
    for listing_url in listing_urls:
        web_content = get_and_historize_url_content(listing_url, load_timestamp)
        job_description_urls.extend(get_job_description_urls(web_content))
    return job_description_urls


def convert_urls_to_df(all_job_description_urls) -> pd.DataFrame:
    df = pd.DataFrame(all_job_description_urls, columns=['url'])

    df = df.drop_duplicates()
    url_split = df['url'].str.split('--', expand=True)
    df['name_slug'] = url_split[1]
    df['id'] = url_split[2].str.split('-', expand=True)[0]
    df = df.sort_values(by=['id'], ascending=False)

    return df


def download_sitemap(load_timestamp) -> pd.DataFrame:
    configure_logger(load_timestamp, 'download_sitemap')
    check_load_timestamp(load_timestamp)
    logger.info('download_sitemap: start')
    all_job_description_urls = get_all_job_description_urls(load_timestamp)
    df = convert_urls_to_df(all_job_description_urls)
    save_temp_df(df, load_timestamp, SITEMAP_URLS_CSV)
    logger.info('download_sitemap: end')
    return df


if __name__ == '__main__':
    download_sitemap(LATEST_LOAD_TIMESTAMP)

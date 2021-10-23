import pandas as pd
import xmltodict

from common.entity import SITEMAP
from common.env_variables import DATA_SOURCE_URL
from common.webclient import get_url_content
from common.storage import save_temp_df, SITEMAP_URLS_CSV, save_raw_file, get_current_date_and_time

SITEMAP_INDEX_XML = f'{DATA_SOURCE_URL}5/sitemaps/de/sitemapindex.xml'


def historize_url_content(url, content):
    file_name = url.split('/')[-1]
    save_raw_file(content, SITEMAP, file_name)


def get_and_historize_url_content(url):
    content = get_url_content(url)
    historize_url_content(url, content)
    return content


def get_listing_urls():
    web_content = get_and_historize_url_content(SITEMAP_INDEX_XML)
    web_content = xmltodict.parse(web_content)
    web_content = web_content['sitemapindex']
    web_content = web_content[SITEMAP]
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


def get_all_job_description_urls():
    listing_urls = get_listing_urls()
    job_description_urls = []
    for listing_url in listing_urls:
        web_content = get_and_historize_url_content(listing_url)
        job_description_urls.extend(get_job_description_urls(web_content))
    return job_description_urls


def save_urls_as_df(all_job_description_urls, job_id):
    df = pd.DataFrame(all_job_description_urls, columns=['url'])

    df = df.drop_duplicates()
    url_split = df['url'].str.split('--', expand=True)
    df['name_slug'] = url_split[1]
    df['id'] = url_split[2].str.split('-', expand=True)[0]
    df = df.sort_values(by=['id'], ascending=False)

    save_temp_df(df, job_id, SITEMAP_URLS_CSV)


def download_sitemap(job_id):
    all_job_description_urls = get_all_job_description_urls()
    save_urls_as_df(all_job_description_urls, job_id)


if __name__ == '__main__':
    download_sitemap(get_current_date_and_time())

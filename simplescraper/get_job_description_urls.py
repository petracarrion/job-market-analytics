import pandas as pd
import xmltodict

from simplescraper.utils.webclient import get_web_content

DATA_RESULTS_URLS_CSV = 'data/results/urls.csv'
DATA_RESULTS_DOWLOADED_URLS_CSV = 'data/results/downloaded_urls.csv'

SITEMAP_INDEX_XML = 'https://www.stepstone.de/5/sitemaps/de/sitemapindex.xml'


def get_all_job_description_urls():
    listing_urls = get_listing_urls()
    job_description_urls = []
    for listing_url in listing_urls:
        job_description_urls.extend(get_job_description_urls(listing_url))
    return job_description_urls


def get_job_description_urls(listing_url):
    print(f'Parsing {listing_url}')
    web_content = get_web_content(listing_url)
    web_content = xmltodict.parse(web_content)
    web_content = web_content['urlset']
    url_entries = web_content['url']
    urls = []
    for entry in url_entries:
        url = entry['loc']
        print(url)
        urls.append(url)

    return urls


def get_listing_urls():
    web_content = get_web_content(SITEMAP_INDEX_XML)
    web_content = xmltodict.parse(web_content)
    web_content = web_content['sitemapindex']
    web_content = web_content['sitemap']
    urls = []
    for entry in web_content:
        url = entry['loc']
        if 'listings' in url:
            urls.append(url)
    return urls


def save_urls_as_df(all_job_description_urls):
    df = pd.DataFrame(all_job_description_urls, columns=['job_url'])
    url_split = df['job_url'].str.split('--', expand=True)
    df['job_name_slug'] = url_split[1]
    df['job_id'] = url_split[2].str.split('-', expand=True)[0]
    df = df.sort_values(by=['job_id'], ascending=False)
    df.to_csv(DATA_RESULTS_URLS_CSV, index=False)


def load_urls_as_df():
    return pd.read_csv(DATA_RESULTS_URLS_CSV)


if __name__ == '__main__':
    all_job_description_urls = get_all_job_description_urls()
    print(all_job_description_urls)

    save_urls_as_df(all_job_description_urls)

# df = load_urls_as_df()
# url_split = df["url"].str.split("--", expand=True)
# df["job_name_slug"] = url_split[1]
# df["job_id"] = url_split[2]
# df = df.sort_values(by=["job_id"], ascending=False)
# df = df.reset_index(drop=True)
# print(df)

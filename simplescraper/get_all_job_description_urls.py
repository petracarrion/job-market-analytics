import requests
import xmltodict as xmltodict

SITEMAP_INDEX_XML = "https://www.stepstone.de/5/sitemaps/de/sitemapindex.xml"


def get_all_job_description_urls():
    listing_urls = get_listing_urls()
    job_description_urls = []
    for listing_url in listing_urls:
        job_description_urls.extend(get_job_description_urls(listing_url))
    return job_description_urls


def get_job_description_urls(listing_url):
    print(f'Parsing {listing_url}')
    response = requests.get(listing_url)
    parsed_response = xmltodict.parse(response.content)
    url_set = parsed_response['urlset']
    url_entries = url_set['url']
    urls = []
    for entry in url_entries:
        url = entry['loc']
        print(url)
        urls.append(url)

    return urls


def get_listing_urls():
    response = requests.get(SITEMAP_INDEX_XML)
    parsed_response = xmltodict.parse(response.content)
    sitemap = parsed_response['sitemapindex']
    sitemap_entries = sitemap['sitemap']
    urls = []
    for entry in sitemap_entries:
        url = entry['loc']
        if 'listings' in url:
            urls.append(url)
    return urls


all_job_description_urls = get_all_job_description_urls()
print(all_job_description_urls)
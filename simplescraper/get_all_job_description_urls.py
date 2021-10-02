import requests
import xmltodict as xmltodict

SITEMAP_INDEX_XML = "https://www.stepstone.de/5/sitemaps/de/sitemapindex.xml"


def get_listing_urls():
    response = requests.get(SITEMAP_INDEX_XML)
    parsed_response = xmltodict.parse(response.content)
    sitemap = parsed_response['sitemapindex']
    sitemap_entries = sitemap['sitemap']
    urls = []
    for i in sitemap_entries:
        url = i['loc']
        print(url)
        if 'listings' in url:
            urls.append(url)
    return urls


listing_urls = get_listing_urls()
print(listing_urls)


def get_job_description_urls(listing_url):
    print(f'Parsing {listing_url}')
    response = requests.get(listing_url)
    print(response)
    return []


for listing_url in listing_urls:
    job_description_urls = get_job_description_urls(listing_url)
    print(job_description_urls)



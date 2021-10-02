import requests
import xmltodict as xmltodict

SITEMAP_INDEX_XML = "https://www.stepstone.de/5/sitemaps/de/sitemapindex.xml"


def get_listings_url():
    global listing_urls
    response = requests.get(SITEMAP_INDEX_XML)
    parsed_response = xmltodict.parse(response.content)
    sitemap = parsed_response['sitemapindex']
    sitemap_entries = sitemap['sitemap']
    listing_urls = []
    for i in sitemap_entries:
        url = i['loc']
        print(url)
        if 'listings' in url:
            listing_urls.append(url)
    return listing_urls


listing_urls = get_listings_url()
print(listing_urls)




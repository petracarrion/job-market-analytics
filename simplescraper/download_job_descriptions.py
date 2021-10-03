import os
import time

import pandas as pd
from playwright.sync_api import sync_playwright

from simplescraper.get_job_description_urls import DATA_RESULTS_URLS_CSV
from simplescraper.utils.webclient import get_local_path


def open_first_page(browser):
    page = browser.new_page()
    page.goto('https://www.stepstone.de/')
    page.click("#ccmgt_explicit_accept")
    time.sleep(1)
    return page


def download_urls(urls):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        page = open_first_page(browser)
        for url in urls:
            file_path = get_local_path(url)
            if os.path.isfile(file_path):
                continue
            try:
                print(f"Downloading: {url}")
                page.goto(url)
                listing_content = page.query_selector(".listing-content")
                listing_content_html = listing_content.inner_html()
                listing_content_html = listing_content_html.replace('\xad', '')
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(listing_content_html)
            except TimeoutError:
                print(f'TimeoutError: Timeout error while requesting the page {url}')
            except AttributeError:
                print(f'AttributeError: it seems the following URL is gone {url}')

        browser.close()


# def get_urls():
#     urls = """https://www.stepstone.de/stellenangebote--IT-Systemadministrator-Netzwerkadministrator-m-w-d-Greifswald-HanseYachts-AG--7577304-inline.html"""
#     urls = urls.split()
#
#     return urls
#
#
# download_urls(get_urls())

df = pd.read_csv(DATA_RESULTS_URLS_CSV)
df = df[df.job_name_slug.str.contains("Data-Engineer")]
df = df.head(400)
urls = df['job_url'].tolist()
print(urls)
download_urls(urls)

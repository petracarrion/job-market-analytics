import time

from playwright.sync_api import sync_playwright


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
            try:
                page.goto(url)
                listing_content = page.query_selector(".listing-content")
                listing_content_html = listing_content.inner_html()
                listing_content_html = listing_content_html.replace('\xad', '')
                filename = url.split('/')[-1]
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(listing_content_html)
            except TimeoutError:
                print(f'Timeout error while requesting the page {url}')

        browser.close()


def get_urls():
    urls = """https://www.stepstone.de/stellenangebote--IT-Systemadministrator-Netzwerkadministrator-m-w-d-Greifswald-HanseYachts-AG--7577304-inline.html"""
    urls = urls.split()

    return urls


download_urls(get_urls())

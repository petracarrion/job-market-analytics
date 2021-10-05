import asyncio
import os
import time

import pandas as pd
from playwright.async_api import async_playwright, Error, TimeoutError

from simplescraper.get_job_description_urls import DATA_RESULTS_URLS_CSV
from simplescraper.utils.webclient import get_local_path


async def open_first_page(browser):
    page = await browser.new_page()
    await page.goto('https://www.stepstone.de/')
    await page.click('#ccmgt_explicit_accept')
    time.sleep(1)
    return page


async def download_urls(urls):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=250)
        try:
            page = await open_first_page(browser)
            for idx, url in enumerate(urls):
                file_path = get_local_path(url)
                if os.path.isfile(file_path):
                    continue
                try:
                    print(f'Downloading ({idx+1}/{len(urls)}): {url}')
                    await page.goto(url)
                    listing_content = await page.query_selector('.listing-content')
                    listing_content_html = await listing_content.inner_html()
                    listing_content_html = listing_content_html.replace('\xad', '')
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(listing_content_html)
                except TimeoutError:
                    print(f'TimeoutError: Timeout error while requesting the page {url}')
                except AttributeError:
                    print(f'AttributeError: it seems the following URL is gone {url}')
        except Error:
            print('It seems that the browser crashed.')
        finally:
            await browser.close()


# def get_urls():
#     urls = """https://www.stepstone.de/stellenangebote--IT-Systemadministrator-Netzwerkadministrator-m-w-d-Greifswald-HanseYachts-AG--7577304-inline.html"""
#     urls = urls.split()
#
#     return urls
#
#
# download_urls(get_urls())

def split_dataframe(df, chunk_size):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks


sem = asyncio.Semaphore(8)


async def safe_download_urls(urls):
    async with sem:  # semaphore limits num of simultaneous downloads
        return await download_urls(urls)


async def main():
    tasks = [
        asyncio.ensure_future(safe_download_urls(chunk['job_url'].tolist()))  # creating task starts coroutine
        for chunk
        in dfs
    ]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    df = pd.read_csv(DATA_RESULTS_URLS_CSV)
    dfs = split_dataframe(df, 200)

    # df = df[df.job_name_slug.str.contains("Entwickle")]
    # df = df.head(500)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

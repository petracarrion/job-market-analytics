import asyncio
import os
import time

import pandas as pd
from playwright.async_api import async_playwright, Error, TimeoutError

from simplescraper.download_sitemap import DATA_RESULTS_URLS_CSV, DATA_RESULTS_DOWLOADED_URLS_CSV
from simplescraper.utils.logging import get_logger
from simplescraper.utils.webclient import get_local_path

SEMAPHORE_COUNT = 8

logger = get_logger()


async def open_first_page(browser):
    page = await browser.new_page()
    await page.goto('https://www.stepstone.de/')
    await page.click('#ccmgt_explicit_accept')
    time.sleep(1)
    return page


async def download_urls(df):
    if df.empty:
        return
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=250)
        try:
            min = df['position'].min()
            max = df['position'].max()
            chunk_id = f'{min}-{max}'
            chunk_size = df.shape[0]
            logger.info(f'Starting chunk {chunk_id} with size of {chunk_size}')
            start_time = time.time()
            page = await open_first_page(browser)
            url_dicts = df.to_dict('records')
            for url_dict in url_dicts:
                url = url_dict['job_url']
                position = url_dict['position']
                total_count = url_dict['total_count']
                file_path = get_local_path(url)
                if os.path.isfile(file_path):
                    logger.info(f'Skipped {url}')
                    continue
                try:
                    logger.info(f'Downloading ({position}/{total_count}): {url}')
                    await page.goto(url)
                    listing_content = await page.query_selector('.listing-content')
                    listing_content_html = await listing_content.inner_html()
                    listing_content_html = listing_content_html.replace('\xad', '')
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(listing_content_html)
                        logger.info(f'Dowloaded {url}')
                except TimeoutError:
                    logger.warning(f'TimeoutError: Timeout error while requesting the page {url}')
                except AttributeError:
                    logger.warning(f'AttributeError: it seems the following URL is gone {url}')
        except Error:
            logger.error('It seems that the browser crashed.')
        finally:
            await browser.close()

        elapsed_time = time.time() - start_time
        logger.info(f'Finished chunk {chunk_id}')
        logger.info(f'Elapsed time {chunk_id}: {elapsed_time:.2f} seconds')
        logger.info(f'Downloads per second {chunk_id}: {chunk_size / elapsed_time:.2f}')


# def get_urls():
#     urls = """https://www.stepstone.de/stellenangebote--IT-Systemadministrator-Netzwerkadministrator-m-w-d-Greifswald-HanseYachts-AG--7577304-inline.html"""
#     urls = urls.split()
#
#     return urls
#
#
# download_urls(get_urls())

def split_dataframe(df, chunk_size):
    chunks = []
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


sem = asyncio.Semaphore(SEMAPHORE_COUNT)


async def safe_download_urls(urls):
    async with sem:  # semaphore limits num of simultaneous downloads
        return await download_urls(urls)


async def main():
    tasks = [
        asyncio.ensure_future(safe_download_urls(chunk))  # creating task starts coroutine
        for chunk
        in dfs
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    df: pd.DataFrame = pd.read_csv(DATA_RESULTS_URLS_CSV)
    downloaded_df = pd.read_csv(DATA_RESULTS_DOWLOADED_URLS_CSV)

    df = (df.merge(downloaded_df, on='job_url', how='left', indicator=True)
          .query('_merge == "left_only"')
          .drop('_merge', 1))

    df = df.reset_index(drop=True)
    df['position'] = df.index + 1
    total_count = df.shape[0]
    df['total_count'] = total_count

    dfs = split_dataframe(df, 100)

    start_time = time.time()
    logger.info(f'Starting')
    logger.info(f'Concurrent tasks: {SEMAPHORE_COUNT}')
    logger.info(f'Urls to dowload: {total_count}')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    elapsed_time = time.time() - start_time
    logger.info(f'Finished')
    logger.info(f'Elapsed time: {elapsed_time:.2f} seconds')
    logger.info(f'Downloads per second: {total_count / elapsed_time:.2f}')

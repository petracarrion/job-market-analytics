import asyncio
import time

import pandas as pd
from playwright.async_api import async_playwright, Error, TimeoutError

from utils.logging import get_logger
from utils.storage import load_temp_df, DOWNLOADED_URLS_CSV, SITEMAP_URLS_CSV, save_raw_file, raw_files_exists

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
                file_name = url.split('/')[-1]
                if raw_files_exists('stepstone', 'job_description', file_name):
                    logger.info(f'Skipped {url}')
                    continue
                try:
                    logger.info(f'Downloading ({position}/{total_count}): {url}')
                    await page.goto(url)
                    await page.wait_for_selector('.listing-content', timeout=10000, state='attached')
                    listing_content = await page.query_selector('.listing-content')
                    listing_content_html = await listing_content.inner_html()
                    listing_content_html = listing_content_html.replace('\xad', '')
                    save_raw_file(listing_content_html, 'job_description', file_name)
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


async def run_async_tasks(chucks):
    tasks = [
        asyncio.ensure_future(safe_download_urls(chunk))  # creating task starts coroutine
        for chunk
        in chucks
    ]
    await asyncio.gather(*tasks)


def main():
    downloaded_df: pd.DataFrame = load_temp_df(DOWNLOADED_URLS_CSV)
    df: pd.DataFrame = load_temp_df(SITEMAP_URLS_CSV)

    df = (df.merge(downloaded_df, on='job_url', how='left', indicator=True)
          .query('_merge == "left_only"')
          .drop('_merge', 1))
    df = df.reset_index(drop=True)
    df['position'] = df.index + 1
    total_count = df.shape[0]
    df['total_count'] = total_count
    chucks = split_dataframe(df, 100)
    start_time = time.time()
    logger.info(f'Starting')
    logger.info(f'Concurrent tasks: {SEMAPHORE_COUNT}')
    logger.info(f'Urls to dowload: {total_count}')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_async_tasks(chucks))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    elapsed_time = time.time() - start_time
    logger.info(f'Finished')
    logger.info(f'Elapsed time: {elapsed_time:.2f} seconds')
    logger.info(f'Downloads per second: {total_count / elapsed_time:.2f}')


if __name__ == '__main__':
    main()

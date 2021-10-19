import asyncio
import math
import time

import pandas as pd
from playwright.async_api import async_playwright, Error, TimeoutError

from utils.env_variables import DATA_SOURCE_URL
from utils.logging import configure_logger, get_logger
from utils.storage import load_temp_df, DOWNLOADED_URLS_CSV, SITEMAP_URLS_CSV, save_raw_file, save_temp_df, \
    URLS_TO_DOWNLOAD_CSV

SEMAPHORE_COUNT = 1
MAX_CHUNK_SIZE = 500

logger = get_logger()


class PageNotFound(Exception):
    pass


async def open_first_page(browser):
    page = await browser.new_page()
    await page.goto(DATA_SOURCE_URL)
    await page.click('#ccmgt_explicit_accept')
    time.sleep(1)
    return page


async def download_urls(df):
    if df.empty:
        return
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        try:
            chunk_pos = df['chunk_pos'].values[0]
            num_chunks = df['num_chunks'].values[0]
            chunk_size = df['chunk_size'].values[0]
            chunk_id = f'{chunk_pos}/{num_chunks}'
            logger.info(f'Starting chunk {chunk_id} with size of {chunk_size}')
            start_time = time.time()
            page = await open_first_page(browser)
            url_dicts = df.to_dict('records')
            for url_dict in url_dicts:
                url = url_dict['job_url']
                pos_in_chunk = url_dict['pos_in_chunk']
                file_name = url.split('/')[-1]
                try:
                    logger.info(f'Chunk {chunk_id}: Downloading ({pos_in_chunk}/{chunk_size}): {url}')
                    try:
                        response = await page.goto(url)
                        if response.status >= 400 and response.status >= 400 < 500:
                            raise PageNotFound('Page not found')
                        await page.wait_for_selector('.listing-content', timeout=5000, state='attached')
                    except TimeoutError:
                        logger.warning(f'TimeoutError: second try for {url}')
                        await page.goto(url)
                        await page.wait_for_selector('.listing-content', timeout=10000, state='attached')
                    listing_content = await page.query_selector('.listing-content')
                    listing_content_html = await listing_content.inner_html()
                    listing_content_html = listing_content_html.replace('\xad', '')
                    save_raw_file(listing_content_html, 'job_description', file_name)
                    logger.info(f'Chunk {chunk_id}: Dowloaded   ({pos_in_chunk}/{chunk_size}): {url}')
                except TimeoutError:
                    logger.warning(f'TimeoutError: Timeout error while requesting the page {url}')
                except AttributeError:
                    logger.warning(f'AttributeError: it seems the following URL is gone {url}')
                except PageNotFound:
                    logger.warning(f'PageNotFound: the following URL is no longer available {url}')
        except Error:
            logger.error('It seems that the browser crashed.')
        finally:
            await browser.close()

        elapsed_time = time.time() - start_time
        logger.info(f'Finished chunk {chunk_id}')
        logger.info(f'Elapsed time {chunk_id}: {elapsed_time:.2f} seconds')
        logger.info(f'Downloads per second {chunk_id}: {chunk_size / elapsed_time:.2f}')


def split_dataframe(df, chunk_size):
    chunks = []
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunk = df[i * chunk_size:(i + 1) * chunk_size]
        chunk = chunk.reset_index(drop=True)
        chunk['chunk_pos'] = i + 1
        chunk['num_chunks'] = num_chunks
        chunk['pos_in_chunk'] = chunk.index + 1
        chunk['chunk_size'] = chunk.shape[0]
        chunks.append(chunk)
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


def get_chunk_size(total_count):
    chunk_size = total_count / SEMAPHORE_COUNT
    chunk_size = int(math.ceil(chunk_size))
    chunk_size = min(chunk_size, MAX_CHUNK_SIZE)
    return chunk_size


def download_job_descriptions(job_id):
    configure_logger(job_id)

    downloaded_df: pd.DataFrame = load_temp_df(job_id, DOWNLOADED_URLS_CSV)
    df: pd.DataFrame = load_temp_df(job_id, SITEMAP_URLS_CSV)

    df = df.merge(downloaded_df, on='job_url', how='left', indicator=True)
    df = df.query('_merge == "left_only"')
    df = df.drop(columns=['_merge'])
    df = df.reset_index(drop=True)

    save_temp_df(df, job_id, URLS_TO_DOWNLOAD_CSV)

    if df.empty:
        logger.info('Nothing to download')
        exit(0)

    total_count = df.shape[0]
    chunk_size = get_chunk_size(total_count)
    chucks = split_dataframe(df, chunk_size)

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
    download_job_descriptions('TODO: Read it from an env variable or just get the latest job_id')

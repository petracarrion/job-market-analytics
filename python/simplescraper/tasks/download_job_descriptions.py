import asyncio
import time

from playwright.async_api import async_playwright, Error, TimeoutError

from common.chunking import get_chunk_size
from common.entity import JOB_DESCRIPTION
from common.env_variables import DATA_SOURCE_URL, SEMAPHORE_COUNT, MAX_CHUNK_SIZE, LATEST_LOAD_TIMESTAMP, RUN_HEADLESS, \
    MIN_TO_DOWNLOAD, MAX_TO_DOWNLOAD
from common.logging import logger, configure_logger
from common.storage import save_raw_file, load_temp_df, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV

TAB_HITS = 10


class PageNotFound(Exception):
    pass


async def open_first_page(browser):
    page = await browser.new_page()
    await page.goto(DATA_SOURCE_URL, wait_until='domcontentloaded')
    await page.click('#ccmgt_explicit_accept')
    for i in range(TAB_HITS * 2):
        await page.keyboard.press('Tab')
    await page.goto(DATA_SOURCE_URL + 'de/sitemap/', wait_until='domcontentloaded')
    for i in range(TAB_HITS * 2):
        await page.keyboard.press('Tab')
    return page


async def download_urls(df, load_timestamp):
    if df.empty:
        return
    async with async_playwright() as p:
        chunk_pos = df['chunk_pos'].values[0]
        chunk_pos = str(chunk_pos).rjust(2)
        num_chunks = df['num_chunks'].values[0]
        chunk_size = df['chunk_size'].values[0]
        chunk_id = f'{chunk_pos}/{num_chunks}'
        browser = await p.chromium.launch(headless=RUN_HEADLESS, slow_mo=250)
        try:
            logger.info(f'Starting chunk {chunk_id} with size of {chunk_size}')
            start_time = time.time()
            page = await open_first_page(browser)
            url_dicts = df.to_dict('records')
            for url_dict in url_dicts:
                pos_in_chunk = url_dict['pos_in_chunk']
                url = url_dict['url']
                job_id = url.rsplit('--', 1)
                job_id = job_id[1]
                job_id = job_id.split('-')
                job_id = job_id[0]
                file_name = f'{job_id}.html'
                try:
                    logger.debug(f'Chunk {chunk_id}: Downloading ({pos_in_chunk}/{chunk_size}): {url}')
                    try:
                        response = await page.goto(url, wait_until='domcontentloaded')
                        for i in range(TAB_HITS):
                            await page.keyboard.press('Tab')
                        if response.status >= 400 and response.status >= 400 < 500:
                            raise PageNotFound('Page not found')
                        await page.wait_for_selector('.listing-content', timeout=10000, state='attached')
                    except TimeoutError as err:
                        logger.warning(
                            f'Chunk {chunk_id}: TimeoutError: second try for {url} because of the following error: {err}')
                        await page.goto(DATA_SOURCE_URL + 'de/sitemap/', wait_until='domcontentloaded')
                        for i in range(TAB_HITS):
                            await page.keyboard.press('Tab')
                        await page.goto(url, wait_until='domcontentloaded')
                        for i in range(TAB_HITS):
                            await page.keyboard.press('Tab')
                        await page.wait_for_selector('.listing-content', timeout=20000, state='attached')
                    page_content = await page.content()
                    save_raw_file(page_content, JOB_DESCRIPTION, load_timestamp, file_name)
                    logger.success(f'Chunk {chunk_id}: Downloaded  ({pos_in_chunk}/{chunk_size}): {url}')
                except TimeoutError:
                    logger.warning(f'Chunk {chunk_id}: TimeoutError: Timeout error while requesting the page {url}')
                except AttributeError:
                    logger.warning(f'Chunk {chunk_id}: AttributeError: it seems the following URL is gone {url}')
                except PageNotFound:
                    logger.warning(f'Chunk {chunk_id}: PageNotFound: the following URL is no longer available {url}')
        except Error as err:
            logger.error(f'Chunk {chunk_id}: It seems that the browser crashed because of the following error: {err}')
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


async def safe_download_urls(urls, load_timestamp, sem):
    async with sem:  # semaphore limits num of simultaneous downloads
        return await download_urls(urls, load_timestamp)


async def run_async_tasks(chunks, load_timestamp):
    sem = asyncio.Semaphore(SEMAPHORE_COUNT)
    tasks = [
        asyncio.ensure_future(safe_download_urls(chunk, load_timestamp, sem))  # creating task starts coroutine
        for chunk
        in chunks
    ]
    await asyncio.gather(*tasks)


def download_job_descriptions(load_timestamp, df_to_download=None):
    configure_logger(load_timestamp)
    df = df_to_download if df_to_download is not None else load_temp_df(load_timestamp, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV)

    if MAX_TO_DOWNLOAD:
        df = df.head(MAX_TO_DOWNLOAD)

    total_count = df.shape[0]

    if total_count < MIN_TO_DOWNLOAD:
        logger.success(f'Not enough to download: {total_count} for the load timestamp {load_timestamp}')
        return

    chunk_size = get_chunk_size(total_count, SEMAPHORE_COUNT, MAX_CHUNK_SIZE)
    chunks = split_dataframe(df, chunk_size)

    start_time = time.time()
    logger.info(f'Starting downloading job descriptions for job: {load_timestamp}')
    logger.info(f'Concurrent tasks: {SEMAPHORE_COUNT}')
    logger.info(f'Urls to download: {total_count}')

    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_async_tasks(chunks, load_timestamp))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    elapsed_time = time.time() - start_time
    logger.info(f'Elapsed time: {elapsed_time:.2f} seconds')
    logger.info(f'Downloads per second: {total_count / elapsed_time:.2f}')
    logger.success(f'Finished: {total_count} urls for the timestamp {load_timestamp}')


if __name__ == '__main__':
    download_job_descriptions(
        LATEST_LOAD_TIMESTAMP,
        load_temp_df(LATEST_LOAD_TIMESTAMP, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV),
    )

import asyncio
import time

from playwright.async_api import async_playwright, Error, TimeoutError

from common.chunking import get_chunk_size
from common.entity import JOB_DESCRIPTION
from common.env_variables import DATA_SOURCE_URL, SEMAPHORE_COUNT, MAX_CHUNK_SIZE, LATEST_RUN_ID
from common.logging import logger
from common.storage import save_raw_file, load_temp_df, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV

TAB_HITS = 30


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


async def download_urls(df):
    if df.empty:
        return
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=250)
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
                url = url_dict['url']
                pos_in_chunk = url_dict['pos_in_chunk']
                file_name = url.split('/')[-1]
                try:
                    logger.debug(f'Chunk {chunk_id}: Downloading ({pos_in_chunk}/{chunk_size}): {url}')
                    try:
                        response = await page.goto(url, wait_until='domcontentloaded')
                        for i in range(TAB_HITS):
                            await page.keyboard.press('Tab')
                        if response.status >= 400 and response.status >= 400 < 500:
                            raise PageNotFound('Page not found')
                        await page.wait_for_selector('.listing-content', timeout=5000, state='attached')
                    except TimeoutError as err:
                        logger.warning(f'TimeoutError: second try for {url} because of the following error: {err}')
                        await page.goto(DATA_SOURCE_URL + 'de/sitemap/', wait_until='domcontentloaded')
                        for i in range(TAB_HITS):
                            await page.keyboard.press('Tab')
                        await page.goto(url, wait_until='domcontentloaded')
                        for i in range(TAB_HITS):
                            await page.keyboard.press('Tab')
                        await page.wait_for_selector('.listing-content', timeout=10000, state='attached')
                    page_content = await page.content()
                    save_raw_file(page_content, JOB_DESCRIPTION, file_name)
                    logger.success(f'Chunk {chunk_id}: downloaded   ({pos_in_chunk}/{chunk_size}): {url}')
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


async def run_async_tasks(chunks):
    tasks = [
        asyncio.ensure_future(safe_download_urls(chunk))  # creating task starts coroutine
        for chunk
        in chunks
    ]
    await asyncio.gather(*tasks)


def download_job_descriptions(run_id, df_to_download):
    df = df_to_download

    if df.empty:
        logger.info('Nothing to download')
        exit(0)

    total_count = df.shape[0]
    chunk_size = get_chunk_size(total_count, SEMAPHORE_COUNT, MAX_CHUNK_SIZE)
    chunks = split_dataframe(df, chunk_size)

    start_time = time.time()
    logger.info(f'Starting downloading job descriptions for job: {run_id}')
    logger.info(f'Concurrent tasks: {SEMAPHORE_COUNT}')
    logger.info(f'Urls to download: {total_count}')

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_async_tasks(chunks))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    elapsed_time = time.time() - start_time
    logger.success(f'Finished: {total_count} urls')
    logger.info(f'Elapsed time: {elapsed_time:.2f} seconds')
    logger.info(f'Downloads per second: {total_count / elapsed_time:.2f}')


if __name__ == '__main__':
    download_job_descriptions(
        LATEST_RUN_ID,
        load_temp_df(LATEST_RUN_ID, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV),
    )

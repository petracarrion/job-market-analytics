import os

import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.logging import get_logger, configure_logger
from common.storage import get_job_id, load_raw_file, save_cleansed_df
from tasks.chunk_job_descriptions_to_parse import chunk_job_descriptions_to_parse
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.list_job_descriptions_to_parse import list_job_descriptions_to_parse
from tasks.list_parsed_job_descriptions import list_parsed_job_descriptions
from tasks.parse_job_description import parse_job_description

DEBUG = True

logger = get_logger()


def load_and_parse(row) -> str:
    timestamp = row['timestamp']
    file_name = row['file_name']
    chunk_id = row['chunk_id']
    pos_in_chunk = row['pos_in_chunk']
    chunk_size = row['chunk_size']
    html_content = load_raw_file(JOB_DESCRIPTION, timestamp, file_name)
    try:
        logger.debug(f'Parsing ({chunk_id}) {pos_in_chunk}/{chunk_size}: {timestamp}/{file_name}')
        parsed_content = parse_job_description(html_content)
        return parsed_content
    except AttributeError:
        logger.warning(f'The following file could not be parsed: {timestamp}/{file_name}')
        return ''


def parse_job_descriptions():
    job_id = get_job_id()
    configure_logger(job_id)
    df_downloaded = list_downloaded_job_descriptions(job_id)
    df_parsed = list_parsed_job_descriptions(job_id)
    df_to_parse = list_job_descriptions_to_parse(job_id, df_downloaded, df_parsed)
    dfs_to_parse = chunk_job_descriptions_to_parse(job_id, df_to_parse)
    for index, df in enumerate(dfs_to_parse):
        chunk_pos = index + 1
        num_chunks = len(dfs_to_parse)
        chunk_id = f'{chunk_pos}/{num_chunks}'
        chunk_size = df.shape[0]
        df['chunk_id'] = chunk_id
        df = df.reset_index(drop=True)
        df['pos_in_chunk'] = df.index + 1
        df['chunk_size'] = chunk_size

        logger.info(f'Starting to parse job descriptions in a df with size: {chunk_size}')
        df['parsed_content'] = df.apply(load_and_parse, axis=1)
        df = df.join(pd.json_normalize(df['parsed_content']))
        df = df.drop(columns=['parsed_content'])
        df[['year', 'moth', 'day']] = df['timestamp'].str.split('-', 2, expand=True)
        df = df.drop(columns=['chunk_id', 'pos_in_chunk', 'chunk_size'])

        save_cleansed_df(df, JOB_DESCRIPTION)

    os.system('say -v Zuzana A je to')


if __name__ == "__main__":
    parse_job_descriptions()

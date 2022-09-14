import sys

import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.logging import logger, configure_logger
from common.storage import get_load_timestamp, load_raw_file, save_cleansed_df, get_load_date, LOAD_TIMESTAMP_FORMAT
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.parse_job_description import parse_job_description


def load_and_parse(row) -> str:
    load_timestamp = row['load_timestamp']
    file_name = row['file_name']
    html_content = load_raw_file(JOB_DESCRIPTION, load_timestamp, file_name)
    try:
        logger.debug(f'Parsing {load_timestamp}/{file_name}')
        parsed_content = parse_job_description(html_content)
        return parsed_content
    except AttributeError:
        logger.warning(f'The following file could not be parsed: {load_timestamp}/{file_name}')
        return ''


def cleanse_job_descriptions(load_timestamp, load_date):
    configure_logger(load_timestamp, 'parse_job_descriptions')
    df = list_downloaded_job_descriptions(load_timestamp, load_date)
    if df.empty:
        logger.warning(f'Nothing to cleanse for the load date: {load_date}')
        return
    df = df.sort_values(by=['load_timestamp', 'file_name'])
    df = df.reset_index(drop=True)
    logger.info(f'Start  to parse job descriptions for the load date: {load_date}')
    df['parsed_content'] = df.apply(load_and_parse, axis=1)
    df = df.join(pd.json_normalize(df['parsed_content']))
    df = df.drop(columns=['parsed_content'])
    df[['year', 'month', 'day', 'hour']] = df['load_timestamp'].str.split('/', 3, expand=True)
    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'], format=LOAD_TIMESTAMP_FORMAT, utc=True)
    logger.info(f'Finish to parse job descriptions for the load date: {load_date}')
    save_cleansed_df(df, JOB_DESCRIPTION)


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    cleanse_job_descriptions(_load_timestamp, _load_date)

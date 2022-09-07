import pandas as pd

from common.entity import JOB_DESCRIPTION
from common.logging import logger, configure_logger
from common.storage import get_run_timestamp, load_raw_file, save_cleansed_df, get_target_date
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.parse_job_description import parse_job_description


def load_and_parse(row) -> str:
    run_timestamp = row['run_timestamp']
    file_name = row['file_name']
    html_content = load_raw_file(JOB_DESCRIPTION, run_timestamp, file_name)
    try:
        logger.debug(f'Parsing {run_timestamp}/{file_name}')
        parsed_content = parse_job_description(html_content)
        return parsed_content
    except AttributeError:
        logger.warning(f'The following file could not be parsed: {run_timestamp}/{file_name}')
        return ''


def parse_job_descriptions(run_timestamp, target_date):
    configure_logger(run_timestamp, 'parse_job_descriptions')
    df = list_downloaded_job_descriptions(run_timestamp, target_date)
    df = df.sort_values(by=['run_timestamp', 'file_name'])
    df = df.reset_index(drop=True)
    logger.info(f'Start  to parse job descriptions for the target date: {target_date}')
    df['parsed_content'] = df.apply(load_and_parse, axis=1)
    df = df.join(pd.json_normalize(df['parsed_content']))
    df = df.drop(columns=['parsed_content'])
    df[['year', 'month', 'day', 'hour']] = df['run_timestamp'].str.split('/', 3, expand=True)
    logger.info(f'Finish to parse job descriptions for the target date: {target_date}')
    save_cleansed_df(df, JOB_DESCRIPTION)


if __name__ == "__main__":
    parse_job_descriptions(get_run_timestamp(), get_target_date())

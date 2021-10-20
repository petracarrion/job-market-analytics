import pandas as pd

from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.parse_job_description import parse_job_description
from utils.logging import get_logger
from utils.storage import get_current_date_and_time, load_raw_file

DEBUG = True

logger = get_logger()


def load_and_parse(row):
    timestamp = row['timestamp']
    file_name = row['file_name']
    html_content = load_raw_file('job_description', timestamp, file_name)
    try:
        logger.info(f'Parsing: {timestamp}/{file_name}')
        parsed_content = parse_job_description(html_content)
        return parsed_content
    except AttributeError as e:
        logger.warning(f'The following file could not be parsed: {timestamp}/{file_name}')
        return ''


def parse_job_descriptions():
    job_id = get_current_date_and_time()
    df = list_downloaded_job_descriptions(job_id)
    if DEBUG:
        df = df[df['timestamp'] == '2021-10-16']
    df['parsed_content'] = df.apply(load_and_parse, axis=1)
    df = df.reset_index(drop=True)
    df = df.join(pd.json_normalize(df['parsed_content']))
    df = df.drop(columns=['parsed_content'])
    print(df)


if __name__ == "__main__":
    parse_job_descriptions()

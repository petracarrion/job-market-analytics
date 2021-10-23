import pandas as pd

from common.entity import JOB_DESCRIPTION
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.parse_job_description import parse_job_description
from common.logging import get_logger
from common.storage import get_current_date_and_time, load_raw_file, save_cleansed_df

DEBUG = True

logger = get_logger()


def load_and_parse(row) -> str:
    timestamp = row['timestamp']
    file_name = row['file_name']
    html_content = load_raw_file(JOB_DESCRIPTION, timestamp, file_name)
    try:
        logger.info(f'Parsing: {timestamp}/{file_name}')
        parsed_content = parse_job_description(html_content)
        return parsed_content
    except AttributeError:
        logger.warning(f'The following file could not be parsed: {timestamp}/{file_name}')
        return ''


def parse_job_descriptions():
    job_id = get_current_date_and_time()
    df: pd.DataFrame = list_downloaded_job_descriptions(job_id)
    if DEBUG:
        df = df.sample(n=20)
    # df = df.reset_index(drop=True)
    df['parsed_content'] = df.apply(load_and_parse, axis=1)
    # df = df.reset_index(drop=True)
    df = df.join(pd.json_normalize(df['parsed_content']))
    df = df.drop(columns=['parsed_content'])
    df[['year', 'moth', 'day']] = df['timestamp'].str.split('-', 2, expand=True)

    save_cleansed_df(df, JOB_DESCRIPTION)

    print(df)


def read_parquet():
    df = pd.read_parquet('../temp/job_descriptions.parquet', engine='pyarrow', columns=['timestamp', 'file_name'])
    print(df)


if __name__ == "__main__":
    parse_job_descriptions()
    read_parquet()

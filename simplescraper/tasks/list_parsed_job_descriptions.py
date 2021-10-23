from common.entity import JOB_DESCRIPTION
from common.storage import load_cleansed_df, save_temp_df, PARSED_JOB_DESCRIPTIONS_CSV, get_job_id


def list_parsed_job_descriptions(job_id):
    df = load_cleansed_df(JOB_DESCRIPTION, columns=['timestamp', 'file_name'])
    save_temp_df(df, job_id, PARSED_JOB_DESCRIPTIONS_CSV)
    return df


if __name__ == "__main__":
    job_id = get_job_id()
    list_parsed_job_descriptions(job_id)

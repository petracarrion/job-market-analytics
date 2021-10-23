from common.env_variables import LATEST_JOB_ID
from common.storage import load_temp_df, DOWNLOADED_JOB_DESCRIPTIONS_CSV, SITEMAP_URLS_CSV, save_temp_df, \
    JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV


def list_job_descriptions_to_download(job_id, df_sitemap_urls, df_downloaded):
    df = df_sitemap_urls
    df = df.merge(df_downloaded, on='url', how='left', indicator=True)
    df = df.query('_merge == "left_only"')
    df = df.drop(columns=['_merge'])
    df = df.reset_index(drop=True)

    save_temp_df(df, job_id, JOB_DESCRIPTIONS_TO_DOWNLOAD_CSV)
    return df


if __name__ == "__main__":
    list_job_descriptions_to_download(
        LATEST_JOB_ID,
        load_temp_df(LATEST_JOB_ID, SITEMAP_URLS_CSV),
        load_temp_df(LATEST_JOB_ID, DOWNLOADED_JOB_DESCRIPTIONS_CSV)
    )
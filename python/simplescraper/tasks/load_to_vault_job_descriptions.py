import os
import sys

import duckdb
import pyarrow.dataset as ds

from common.env_variables import CURATED_DIR, DATA_SOURCE_NAME, DUCKDB_WAREHOUSE_FILE
from common.logging import configure_logger, logger
from common.storage import get_run_timestamp, get_target_date

CREATE_TABLE_HUB_JOB_DESCRIPTION = '''
    CREATE TABLE IF NOT EXISTS hub_job_description (
         job_hk VARCHAR
        ,load_timestamp TIMESTAMP
        ,job_id NUMERIC
        )
    '''

CREATE_TABLE_SAT_JOB_DESCRIPTION = '''
    CREATE TABLE IF NOT EXISTS sat_job_description (
         job_hk VARCHAR
        ,job_hashdiff VARCHAR
        ,load_timestamp TIMESTAMP
        ,title VARCHAR
        ,online_status VARCHAR
        ,is_anonymous BOOLEAN
        ,should_display_early_applicant BOOLEAN
        ,contract_type VARCHAR
        ,work_type VARCHAR
        ,online_since VARCHAR
        ,description_introduction VARCHAR
        ,description_responsabilities VARCHAR
        ,description_requirements VARCHAR
        ,description_perks VARCHAR
        )
    '''


def load_to_vault_job_descriptions(run_timestamp, target_date):
    configure_logger(run_timestamp, 'load_to_vault_job_descriptions')
    logger.info(f'Start load_to_vault_job_descriptions: {target_date}')

    conn = duckdb.connect(DUCKDB_WAREHOUSE_FILE)

    conn.execute(CREATE_TABLE_HUB_JOB_DESCRIPTION)
    conn.execute(CREATE_TABLE_SAT_JOB_DESCRIPTION)

    parquet_input = os.path.join(CURATED_DIR, DATA_SOURCE_NAME, 'job_description')
    dataset = ds.dataset(parquet_input, format='parquet', partitioning='hive')
    conn.register('''curated_job_description''', dataset)
    year, month, day = target_date.split('/', 2)
    df = conn.execute(f'''    
    CREATE TEMP TABLE staging_job_description AS
        SELECT *
            FROM curated_job_description
            WHERE
                year = {year} AND
                month = {month} AND
                day = {day};
    
    INSERT INTO hub_job_description 
        SELECT
             a.job_hk
            ,'{year}-{month}-{day}'
            ,a.job_id
        FROM staging_job_description a
        LEFT OUTER JOIN hub_job_description b
            ON (a.job_hk = b.job_hk)
        WHERE b.job_hk IS NULL;
         
    INSERT INTO sat_job_description 
        SELECT
             a.job_hk
            ,a.job_hashdiff
            ,'{year}-{month}-{day}'
            ,a.title
            ,a.online_status
            ,a.is_anonymous
            ,a.should_display_early_applicant
            ,a.contract_type
            ,a.work_type
            ,a.online_since
            ,a.description_introduction
            ,a.description_responsabilities
            ,a.description_requirements
            ,a.description_perks
        FROM staging_job_description a
        LEFT OUTER JOIN sat_job_description b
            ON (
                a.job_hk = b.job_hk AND
                a.job_hashdiff = b.job_hashdiff
            )
        WHERE
            b.job_hk IS NULL;
    ''').df()
    print(df)
    df = conn.execute(f'''
    SELECT * FROM hub_job_description;
    ''').df()
    print(df)
    df = conn.execute(f'''
    SELECT * FROM sat_job_description;
    ''').df()
    print(df)

    conn.close()


if __name__ == "__main__":
    _run_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_run_timestamp()
    _target_date = sys.argv[2] if len(sys.argv) > 2 else get_target_date()
    load_to_vault_job_descriptions(_run_timestamp, _target_date)

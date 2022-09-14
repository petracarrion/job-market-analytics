import os
import sys

import duckdb
import pyarrow.dataset as ds

from common.env_variables import CURATED_DIR, DATA_SOURCE_NAME, DUCKDB_WAREHOUSE_FILE
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_target_date

CREATE_TABLE_HUB_JOB_DESCRIPTION = '''
    CREATE TABLE IF NOT EXISTS hub_job_description (
         job_id NUMERIC
        ,load_timestamp TIMESTAMP
        )
    '''

CREATE_TABLE_SAT_JOB_DESCRIPTION = '''
    CREATE TABLE IF NOT EXISTS sat_job_description (
         job_id VARCHAR
        ,job_hashdiff VARCHAR
        ,load_timestamp TIMESTAMP
        ,title VARCHAR
        ,online_status VARCHAR
        ,is_anonymous BOOLEAN
        ,should_display_early_applicant BOOLEAN
        ,contract_type VARCHAR
        ,work_type VARCHAR
        ,online_date VARCHAR
        ,description_introduction VARCHAR
        ,description_responsabilities VARCHAR
        ,description_requirements VARCHAR
        ,description_perks VARCHAR
        )
    '''


def load_to_vault_job_descriptions(load_timestamp, target_date):
    configure_logger(load_timestamp, 'load_to_vault_job_descriptions')
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
             a.job_id
            ,'{year}-{month}-{day}'
        FROM staging_job_description a
        LEFT OUTER JOIN hub_job_description b
            ON (a.job_id = b.job_id)
        WHERE b.job_id IS NULL;
         
    INSERT INTO sat_job_description 
        SELECT
             a.job_id
            ,a.job_hashdiff
            ,'{year}-{month}-{day}'
            ,a.title
            ,a.online_status
            ,a.is_anonymous
            ,a.should_display_early_applicant
            ,a.contract_type
            ,a.work_type
            ,a.online_date
            ,a.description_introduction
            ,a.description_responsabilities
            ,a.description_requirements
            ,a.description_perks
        FROM staging_job_description a
        LEFT OUTER JOIN sat_job_description b
            ON (
                a.job_id = b.job_id AND
                a.job_hashdiff = b.job_hashdiff
            )
        WHERE
            b.job_id IS NULL;
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
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _target_date = sys.argv[2] if len(sys.argv) > 2 else get_target_date()
    load_to_vault_job_descriptions(_load_timestamp, _target_date)

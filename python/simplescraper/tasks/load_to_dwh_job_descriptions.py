import os
import sys

import duckdb
import pyarrow.dataset as ds

from common.env_variables import CURATED_DIR, DATA_SOURCE_NAME, DUCKDB_WAREHOUSE_FILE
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_load_date

CREATE_TABLE_SRC_JOB_DESCRIPTION = '''
    CREATE TABLE IF NOT EXISTS src_job_description (
         job_id VARCHAR
        ,load_timestamp TIMESTAMP
        ,job_hashdiff VARCHAR
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


def load_to_vault_job_descriptions(load_timestamp, load_date):
    configure_logger(load_timestamp, 'load_to_vault_job_descriptions')
    logger.info(f'Start load_to_dwh_job_descriptions: {load_date}')

    conn = duckdb.connect(DUCKDB_WAREHOUSE_FILE)

    conn.execute(CREATE_TABLE_SRC_JOB_DESCRIPTION)

    parquet_input = os.path.join(CURATED_DIR, DATA_SOURCE_NAME, 'job_description')
    dataset = ds.dataset(parquet_input, format='parquet', partitioning='hive')
    conn.register('''curated_job_description''', dataset)
    year, month, day = load_date.split('/', 2)

    conn.execute(f'''    
    CREATE TEMP TABLE tmp_job_description AS
        SELECT *
            FROM curated_job_description
            WHERE
                year = {year} AND
                month = {month} AND
                day = {day};
    
    INSERT INTO src_job_description 
        SELECT
             a.job_id
            ,a.load_timestamp
            ,a.job_hashdiff
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
        FROM tmp_job_description a
        LEFT OUTER JOIN src_job_description b
            ON (
                a.job_id = b.job_id
            )
        WHERE
            b.job_id IS NULL;
    ''')

    logger.info(f'End   load_to_dwh_job_descriptions: {load_date}')

    conn.close()


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    load_to_vault_job_descriptions(_load_timestamp, _load_date)

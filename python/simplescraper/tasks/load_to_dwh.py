import os
import sys

import duckdb
import pyarrow.dataset as ds

from common.entity import CURATED_ENTITIES
from common.env_variables import CURATED_DIR, DATA_SOURCE_NAME, DUCKDB_WAREHOUSE_FILE
from common.logging import configure_logger, logger
from common.storage import get_load_timestamp, get_load_date


def load_to_dwh(load_timestamp, load_date):
    configure_logger(load_timestamp)
    logger.info(f'Start load_to_dwh_job_descriptions: {load_date}')

    conn = duckdb.connect(DUCKDB_WAREHOUSE_FILE)

    for entity in CURATED_ENTITIES:
        conn.execute(f'''
        CREATE TABLE IF NOT EXISTS src_{entity.name} (
             {entity.get_src_clause_create_columns()}
            )
        ''')

        parquet_input = os.path.join(CURATED_DIR, DATA_SOURCE_NAME, entity.name)
        dataset = ds.dataset(parquet_input, format='parquet', partitioning='hive')
        conn.register(f'curated_{entity.name}', dataset)
        year, month, day = load_date.split('/', 2)

        conn.execute(f'''    
        CREATE TEMP TABLE tmp_{entity.name} AS
            SELECT *
                FROM curated_{entity.name}
                WHERE
                    year = {year} AND
                    month = {month} AND
                    day = {day};
        ''')

        conn.execute(f'''
        INSERT INTO src_{entity.name}
            SELECT {entity.get_src_select_columns()}
            FROM tmp_{entity.name} a
            LEFT OUTER JOIN src_{entity.name} b
                ON ( {entity.get_src_clause_join_on()} )
            WHERE
                {entity.get_src_clause_where()};
        ''')

    logger.info(f'End   load_to_dwh_job_descriptions: {load_date}')

    conn.close()


if __name__ == "__main__":
    _load_timestamp = sys.argv[1] if len(sys.argv) > 1 else get_load_timestamp()
    _load_date = sys.argv[2] if len(sys.argv) > 2 else get_load_date()
    load_to_dwh(_load_timestamp, _load_date)

import os

import duckdb

from common.entity import CURATED_ENTITIES
from common.env_variables import CURATED_DIR, DATA_SOURCE_NAME, DUCKDB_DWH_FILE


def create_curated_views_in_dwh():
    conn = duckdb.connect(DUCKDB_DWH_FILE)

    for entity in CURATED_ENTITIES:
        curated_path = os.path.join(CURATED_DIR, DATA_SOURCE_NAME, entity.name, '*/*/*/*.parquet')

        conn.execute(f'''    
        CREATE OR REPLACE view curated_{entity.name} AS
            SELECT * FROM parquet_scan('{curated_path}', HIVE_PARTITIONING=1);
        ''')

    conn.close()


if __name__ == "__main__":
    create_curated_views_in_dwh()

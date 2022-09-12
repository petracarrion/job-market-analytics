import os

import duckdb
import pyarrow.dataset as ds

from common.env_variables import CURATED_DIR, DATA_SOURCE_NAME

parquet_input = os.path.join(CURATED_DIR, DATA_SOURCE_NAME, 'job_online')
print()

con = duckdb.connect()

dataset = ds.dataset(parquet_input, format='parquet', partitioning='hive')
con.register('''Hierarchy''', dataset)
print(con.execute('''
SELECT * FROM Hierarchy
  WHERE
    year = 2022 AND
    month = 9 AND
    day = 1
''').df())

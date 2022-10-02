import duckdb
import pandas as pd
from IPython.display import display

from common.env_variables import DUCKDB_DWH_FILE


def display_df(_df):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, "expand_frame_repr", False,
                           "display.float_format", '${:,.2f}'.format):
        display(_df.fillna('.'))


def display_sql(sql_statement):
    conn = duckdb.connect(DUCKDB_DWH_FILE, read_only=True)
    _df = conn.execute(sql_statement).df()
    conn.close()
    return _df

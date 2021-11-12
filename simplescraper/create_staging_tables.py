import psycopg2

from common.entity import Entity, SITEMAP, JOB_DESCRIPTION
from common.env_variables import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
from common.storage import list_cleansed_files

PARQUET_DFW_DIR = '/var/lib/parquet-fdw/data/'


def create_db_connection():
    return psycopg2.connect(
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )


def create_staging_tables(conn, entity: Entity):
    column_list = [f'{column.column_name} {column.column_type}' for column in entity.stg_columns]
    column_list = ', '.join(column_list)

    file_list = list_cleansed_files(entity)
    file_list = [f'{PARQUET_DFW_DIR}{entity.name}/{file_path}' for file_path in file_list]
    file_list = sorted(file_list)
    file_list = ' '.join(file_list)

    drop_table_statmment = f'drop foreign table if exists stg_{entity.name};'
    create_table_stamment = f'''
    create foreign table stg_{entity.name}
        (
        {column_list}
        )
        server parquet_srv
        options (filename '{file_list}');
        '''

    cursor = conn.cursor()
    cursor.execute(drop_table_statmment)
    cursor.execute(create_table_stamment)

    conn.commit()


if __name__ == "__main__":
    _conn = create_db_connection()
    create_staging_tables(_conn, SITEMAP)
    create_staging_tables(_conn, JOB_DESCRIPTION)
    _conn.close()

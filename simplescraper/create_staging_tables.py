import psycopg2

from common.entity import Entity, SITEMAP, JOB_DESCRIPTION
from common.storage import list_cleansed_files

PARQUET_DFW_DIR = '/var/lib/parquet-fdw/data/'


def create_staging_tables(entity: Entity):
    column_list = [f'{column.column_name} {column.column_type}' for column in entity.stg_columns]
    column_list = ', '.join(column_list)

    file_list = list_cleansed_files(entity)
    file_list = [f'{PARQUET_DFW_DIR}{entity.name}/{file_path}' for file_path in file_list]
    file_list = ' '.join(file_list)

    sql_stamment = f'''
    create foreign table stg_{entity.name}
        (
        {column_list}
        )
        server parquet_srv
        options (filename '{file_list}');
        '''

    print(sql_stamment)


if __name__ == "__main__":
    create_staging_tables(SITEMAP)
    create_staging_tables(JOB_DESCRIPTION)

from typing import List


class Entity:
    def __init__(self, name, src_columns=None):
        self.name = name
        self.src_columns: List[Column] = src_columns

    def __str__(self):
        return self.name

    def get_src_clause_create_columns(self):
        clause = ', '.join([f'{c.column_name} {c.column_type}' for c in self.src_columns])
        return clause

    def get_src_select_columns(self):
        clause = ', '.join([f'a.{c.column_name}' for c in self.src_columns])
        return clause

    def get_src_clause_join_on(self):
        clause = ' and '.join([f'a.{c.column_name} = b.{c.column_name}' for c in self.src_columns if c.is_pk])
        return clause

    def get_src_clause_where(self):
        clause = ' and '.join([f'b.{c.column_name} IS NULL ' for c in self.src_columns if c.is_pk])
        return clause


class Column:
    def __init__(self, column_name, column_type, is_pk=False):
        self.column_name = column_name
        self.column_type = column_type
        self.is_pk = is_pk


SRC_COLUMNS_JOB_ONLINE = [
    Column('job_id', 'VARCHAR', True),
    Column('load_timestamp', 'TIMESTAMP'),
    Column('online_at', 'DATE'),
    Column('url', 'VARCHAR'),
    Column('job_online_hashdiff', 'VARCHAR', True),
]

SRC_COLUMNS_JOB_DESCRIPTION = [
    Column('job_id', 'VARCHAR', True),
    Column('load_timestamp', 'TIMESTAMP'),
    Column('title', 'VARCHAR'),
    Column('online_status', 'VARCHAR'),
    Column('is_anonymous', 'BOOLEAN'),
    Column('should_display_early_applicant', 'BOOLEAN'),
    Column('contract_type', 'VARCHAR'),
    Column('work_type', 'VARCHAR'),
    Column('online_date', 'VARCHAR'),
    Column('description_introduction', 'VARCHAR'),
    Column('description_responsabilities', 'VARCHAR'),
    Column('description_requirements', 'VARCHAR'),
    Column('description_perks', 'VARCHAR'),
    Column('job_hashdiff', 'VARCHAR', True),
]

SRC_COLUMNS_JOB_LOCATION = [
    Column('job_id', 'VARCHAR', True),
    Column('load_timestamp', 'TIMESTAMP'),
    Column('location', 'VARCHAR', True),
]

SITEMAP = Entity('sitemap')
JOB_ONLINE = Entity('job_online', SRC_COLUMNS_JOB_ONLINE)
JOB_DESCRIPTION = Entity('job_description', SRC_COLUMNS_JOB_DESCRIPTION)
JOB_LOCATION = Entity('job_location', SRC_COLUMNS_JOB_LOCATION)

RAW_ENTITIES = [SITEMAP, JOB_DESCRIPTION]
CURATED_ENTITIES = [JOB_ONLINE, JOB_DESCRIPTION, JOB_LOCATION]

if __name__ == "__main__":
    for entity in CURATED_ENTITIES:
        entity.get_src_clause_join_on()

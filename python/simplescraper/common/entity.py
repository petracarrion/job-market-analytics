class Entity:
    def __init__(self, name, stg_columns=None):
        self.name = name
        self.stg_columns = stg_columns

    def __str__(self):
        return self.name


class Column:
    def __init__(self, column_name, column_type):
        self.column_name = column_name
        self.column_type = column_type


STG_COLUMNS_SITEMAP = [
    Column('sitemap_ingestion_hashkey', 'text'),
    Column('run_timestamp', 'text'),
    Column('job_id', 'text'),
    Column('url', 'text'),
]
STG_COLUMNS_JOB_DESCRIPTION = [
    Column('job_description_ingestion_hashkey', 'text'),
    Column('run_timestamp', 'text'),
    Column('job_id', 'text'),
    Column('url', 'text'),
    Column('title', 'text'),
    Column('company_name', 'text'),
    Column('location', 'text'),
    Column('work_type', 'text'),
    Column('description', 'text'),
    Column('file_name', 'text'),
]

SITEMAP = Entity('sitemap', STG_COLUMNS_SITEMAP)
JOB_DESCRIPTION = Entity('job_description', STG_COLUMNS_JOB_DESCRIPTION)
JOB_ONLINE = Entity('job_online')
JOB = Entity('job')
COMPANY = Entity('company')
JOB_COMPANY = Entity('job_company')
LOCATION = Entity('location')
JOB_LOCATION = Entity('job_location')

ALL_ENTITIES = [SITEMAP, JOB_DESCRIPTION]

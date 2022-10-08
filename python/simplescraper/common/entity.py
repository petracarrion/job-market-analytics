from typing import List


class Entity:
    def __init__(self, name, src_columns=None):
        self.name = name
        self.src_columns: List[Column] = src_columns

    def __str__(self):
        return self.name


class Column:
    def __init__(self, column_name, column_type):
        self.column_name = column_name
        self.column_type = column_type


SRC_COLUMNS_ONLINE_JOB = [
    Column('job_id', 'INTEGER'),
    Column('load_timestamp', 'TIMESTAMP'),
    Column('online_at', 'DATE'),
    Column('url', 'VARCHAR'),
]

SRC_COLUMNS_JOB = [
    Column('job_id', 'INTEGER'),
    Column('load_timestamp', 'TIMESTAMP'),
    Column('title', 'VARCHAR'),
    Column('online_status', 'VARCHAR'),
    Column('is_anonymous', 'BOOLEAN'),
    Column('should_display_early_applicant', 'BOOLEAN'),
    Column('contract_type', 'VARCHAR'),
    Column('work_type', 'VARCHAR'),
    Column('online_date', 'VARCHAR'),
    # Column('description_introduction', 'VARCHAR'),
    # Column('description_responsabilities', 'VARCHAR'),
    # Column('description_requirements', 'VARCHAR'),
    # Column('description_perks', 'VARCHAR'),
]

SRC_COLUMNS_JOB_LOCATION = [
    Column('job_id', 'INTEGER'),
    Column('load_timestamp', 'TIMESTAMP'),
    Column('location', 'VARCHAR'),
]

SITEMAP = Entity('sitemap')
ONLINE_JOB = Entity('online_job', SRC_COLUMNS_ONLINE_JOB)
JOB_DESCRIPTION = Entity('job_description')
JOB = Entity('job', SRC_COLUMNS_JOB)
JOB_LOCATION = Entity('job_location', SRC_COLUMNS_JOB_LOCATION)
JOB_TECHNOLOGY = Entity('job_technology')

RAW_ENTITIES = [SITEMAP, JOB_DESCRIPTION]
CURATED_ENTITIES = [ONLINE_JOB, JOB, JOB_LOCATION]

if __name__ == "__main__":
    for entity in CURATED_ENTITIES:
        print(entity)

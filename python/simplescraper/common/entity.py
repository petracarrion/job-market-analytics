class Entity:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


SITEMAP = Entity('sitemap')
ONLINE_JOB = Entity('online_job')
JOB_DESCRIPTION = Entity('job_description')
JOB = Entity('job')
JOB_LOCATION = Entity('job_location')
JOB_TECHNOLOGY = Entity('job_technology')

RAW_ENTITIES = [
    SITEMAP,
    JOB_DESCRIPTION,
]
CURATED_ENTITIES = [
    ONLINE_JOB,
    JOB,
    JOB_LOCATION,
    JOB_TECHNOLOGY,
]

if __name__ == "__main__":
    for entity in CURATED_ENTITIES:
        print(entity)

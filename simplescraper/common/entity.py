
class Entity:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


SITEMAP = Entity('sitemap')
JOB_DESCRIPTION = Entity('job_description')

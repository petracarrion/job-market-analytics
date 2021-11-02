# TO DO

## Open

- [ ] Find a way to pass the list of parquet files to PostgreSQL.
  - Options:
    1. Use Python to create the staging fdw staging tables referencing the parquet files
    2. Remove the partitioning so that all parquet files for an entity are right bellow only one folder
    3. Create a plsql function to find the files recursively
    4. Extend parquet_fdw to be able to scan a folder recursively
    5. ...
- [ ] Add a check for the network connection before we start crawling
- [ ] Add orchestration with Airflow
- [ ] Create the Data Vault

## Done

- [x] Add the _job_id_ to the _sitemap_ and _job_description_ on the cleansed layer
- [x] Create a _ingestion_id_ with the hash of the _job_id_ and _timestap_ on the cleansed layer

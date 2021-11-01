# TO DO

## Open
- [ ] Find a way to pass the list of parquet files to PostgreSQL
- [ ] Add a check for the network connection before we start crawling
- [ ] Add orchestration with Airflow
- [ ] Create the Data Vault

## Done
- [x] Add the _job_id_ to the _sitemap_ and _job_description_ on the cleansed layer
- [x] Create a _ingestion_id_ with the hash of the _job_id_ and _timestap_ on the cleansed layer

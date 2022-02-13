# TO DO

## Open

- [ ] Do not let Flasky start a process behind an endpoint, if a process is still running
- [ ] Allow one retry after the browser crashes
- [ ] Add a file in the raw layer with the scrape run information for each execution
    - This file could be in JSON format and have the following fields:
        - run_id
        - timestamp
        - number of urls to download
        - number of urls downloaded
        - number of failed urls
        - failed urls (a list of string)
- [ ] Implement use case: Technology trends
- [ ] Implement use case: Number of jobs relative to city population
- [ ] Replace the PostgreSQL ingestion with CSV instead of Parquet

## In Progress

- [ ] Create a report that shows how many job offers are online at a given time

## Done

- [x] Find a better timestamp to use than the logical timestamp for the scrape data source dag
- [x] Fix bug with file names longer than 255 characters
- [x] Fix logs in Flasky
- [x] Add more granularity to the ingestion time in the raw data
- [x] Add orchestration with Airflow
- [x] Create the Data Vault
- [x] Optimize the function to create the chunks
- [x] Add a check for the network connection before we start crawling
- [x] Save the whole html document from the source instead of just a fragment of it, so that no information is lost if
  the HTML format changes
- [x] Add logging to the sitemap scraper
- [x] Find a way to pass the list of parquet files to PostgreSQL.
    - Result: Use Python to create the staging fdw staging tables referencing the parquet files
- [x] Add the _job_id_ to the _sitemap_ and _job_description_ on the cleansed layer
- [x] Create a _ingestion_id_ with the hash of the _job_id_ and _timestap_ on the cleansed layer

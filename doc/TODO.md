# TO DO

## Open

- [ ] Upload only backup files to the Azure Blob Storage
- [ ] Implement use case: Compare technologies
- [ ] Implement use case: Number of jobs relative to city population
- [ ] Add the flag to the do and verify backup commands: --exclude='.DS_Store'
- [ ] Add a file in the raw layer with the scrape run information for each execution
    - This file could be in JSON format and have the following fields:
        - run_id
        - timestamp
        - number of urls to download
        - number of urls downloaded
        - number of failed urls
        - failed urls (a list of string)

## In Progress

- [ ] Use statefuls URLs according to state of the input components on Dashy

## Done

- [x] Use LocalExecutor in Airflow
- [x] Run Airflow locally to reduce the Docker overhead
- [x] Implement use case: Technology trends
- [x] Add a size indicator in the filter options in Dashy
- [x] Implement some kind of search/dashboard for external users
- [x] Check out https://github.com/rilldata/rill-developer
- [x] Decide for a BI tool
- [x] Check out https://superset.apache.org/
- [x] Create a separated virtual environment for dbt
- [x] Check out https://www.linkedin.com/in/christian-kaul/recent-activity/posts/
- [x] Check out https://dbtvault.readthedocs.io/
- [x] Check out https://github.com/jwills/dbt-duckdb
- [x] Use Gunicorn to run flasky with 4 workers
- [x] On the cleansed layer, add the first sitemap occurance per URL instead of only the latest load_timestamp
- [x] Add load_timestamp and load_date to the curated layer
- [x] Rename target_date to load_date
- [x] Rename run_timestamp to load_timestamp
- [x] Fail the download sitemap task in the hourly dag if the load_timestamp is older than one hour
- [x] Create a separated virtual environment for airflow
- [x] Fix the issue "metaData-bag.log"
- [x] Find a better way to avoid Airflow to hang when there are many jobs to download
- [x] Move the raw storage to the cloud
- [x] Improve logging
    - Log how many urls to download are
    - Make the check vpn more visible
- [x] Download the job description again after a configurable number of days online
- [x] Create a report that shows how many days a job offer is online
- [x] Create a report that shows how many job offers are online at a given time
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

---

## Discarded

- [x] Try https://xapian.org/ for the search
- [x] Replace the PostgreSQL ingestion with CSV instead of Parquet
- [x] Do not let Flasky start a process behind an endpoint, if a process is still running
- [x] Try Prefect
- [x] Log the date and time more visible
- [x] Allow one retry after the browser crashes

## Technical Debt

- [ ] Rename job_description to job_offer
- [ ] Rename cleansed to curated

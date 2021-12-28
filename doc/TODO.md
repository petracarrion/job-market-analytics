# TO DO

## Open

- [ ] Add a check for the network connection before we start crawling
- [ ] Add orchestration with Airflow
- [ ] Create a report that shows how many job offers are online at a given date
- [ ] Allow one retry after the browser crashes
- [ ] Add more granularity to the ingestion time in the raw data
    - At least at the second level using the ingestion task start time
- [ ] Add a file in the raw layer with the scrape run information for each execution
    - This file could be in JSON format and have the following fields:
        - run_id
        - timestamp
        - number of urls to download
        - number of urls downloaded
        - number of failed urls
        - failed urls (a list of string)
- [ ] Optimize the function to create the chunks

## In Progress

- [ ] Create the Data Vault

## Done

- [x] Save the whole html document from the source instead of just a fragment of it, so that no information is lost if
  the HTML format changes
- [x] Add logging to the sitemap scraper
- [x] Find a way to pass the list of parquet files to PostgreSQL.
    - Result: Use Python to create the staging fdw staging tables referencing the parquet files
- [x] Add the _job_id_ to the _sitemap_ and _job_description_ on the cleansed layer
- [x] Create a _ingestion_id_ with the hash of the _job_id_ and _timestap_ on the cleansed layer

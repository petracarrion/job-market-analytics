#!/bin/bash

source .env

mkdir "${JOB_RESULTS_DIR}"

python list_downloaded_urls.py
#python get_job_descriptions_urls.py
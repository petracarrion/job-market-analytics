#!/bin/bash

SECONDS=0

source .env

python tasks/list_downloaded_urls.py
python tasks/download_sitemap.py
python tasks/download_job_descriptions.py

duration=$SECONDS
echo "${0##*/}: $((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

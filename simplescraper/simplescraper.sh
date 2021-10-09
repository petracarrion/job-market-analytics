#!/bin/bash

source .env

python tasks/list_downloaded_urls.py
python tasks/download_sitemap.py
python tasks/download_job_descriptions.py
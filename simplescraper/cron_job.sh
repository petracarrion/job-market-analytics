#!/bin/bash

# Add the following to the cron jobs: 42 * * * * REPLACE_ME/cron_job.sh

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

"${VENV_PYTHON}" "${SOURCE_DIR}"/simplescraper/scrape_data_source.py

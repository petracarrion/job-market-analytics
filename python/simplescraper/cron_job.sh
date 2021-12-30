#!/bin/bash

# Add the following to the cron jobs: 42 * * * * REPLACE_ME/cron_job.sh

/usr/sbin/scutil --nc list | grep Connected | grep vpn || {
  echo "Please connect to the VPN"
  exit 1
}

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

"${VENV_PYTHON}" "${SOURCE_DIR}"/simplescraper/scrape_data_source.py

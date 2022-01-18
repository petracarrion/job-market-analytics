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

flask run --host=0.0.0.0 --port=3001  --without-threads

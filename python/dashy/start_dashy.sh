#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

gunicorn --workers 1 --timeout 600 --bind 0.0.0.0:8051 dashy:server --access-logfile '-'

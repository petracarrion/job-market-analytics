#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

ulimit -n 2048
gunicorn --workers 8 --timeout 3600 --bind 0.0.0.0:3001 'flasky:app'

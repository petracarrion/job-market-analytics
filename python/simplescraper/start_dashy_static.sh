#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

ulimit -n 4096
gunicorn --workers 4 --timeout 3600 --bind 0.0.0.0:8054 'dashy_static:app'
